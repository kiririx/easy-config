package ec

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

var defaultModule = "DEFAULT"

type Item struct {
	Module string
	Key    string
	Value  string
}

type StorageType int

var (
	MySQL      StorageType = 0
	Properties StorageType = 1
)

type Storage interface {
	getStorage() StorageType
	init(module string) Handler
}

type PropertiesStorage struct {
	path     string
	fileName string
}

func NewPropertiesStorage(path string) Storage {
	return &PropertiesStorage{path: path}
}

func (receiver *PropertiesStorage) getStorage() StorageType {
	return Properties
}

func (receiver *PropertiesStorage) init(module string) Handler {
	configMap, err := resolveProperties(receiver.path)
	if err != nil {
		configMap = make(map[string]string)
	}
	return &PropertiesHandler{module: module, configMap: &configMap, path: receiver.path}
}

type MySQLStorage struct {
	host     string
	port     int
	user     string
	pass     string
	database string
	table    string
}

func (receiver *MySQLStorage) getStorage() StorageType {
	return MySQL
}

var dbCache = &sync.Map{}

func getDbCacheKey(module, key string) string {
	return fmt.Sprintf("%s:%s", module, key)
}

func (receiver *MySQLStorage) init(module string) Handler {
	// 数据库连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", receiver.user, receiver.pass, receiver.host, receiver.port, receiver.database)

	// 连接数据库
	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	// 测试数据库连接
	if err := db.Ping(); err != nil {
		panic(err)
	}
	log.Println("[easy-config] Successfully connected to the database!")
	// 检查表是否存在
	tableName := receiver.table
	if !checkTableExists(db, tableName) {
		createTable(db, tableName)
	}
	return &MySQLHandler{
		module:    module,
		db:        db,
		tableName: tableName,
		cache:     &sync.Map{},
	}
}

func NewMySQLStorage(host string, port int, user, passwd, database string) *MySQLStorage {
	tableName := "EASY_CONFIG_ITEMS"
	return &MySQLStorage{
		host:     host,
		port:     port,
		user:     user,
		pass:     passwd,
		database: database,
		table:    tableName,
	}
}

// Initialize 入口函数
func Initialize(storage Storage, module string) Handler {
	if strings.TrimSpace(module) == "" {
		module = defaultModule
	}
	return storage.init(module)
}

type Handler interface {
	Get(key string) string
	Set(key string, value string) error
	Remove(key string)
	List() []Item
}

type MySQLHandler struct {
	module    string
	db        *sql.DB
	tableName string
	cache     *sync.Map
}

func (h *MySQLHandler) Get(key string) string {
	v, ok := h.cache.Load(key)
	if ok {
		return v.(string)
	}
	// 执行查询
	rows, err := h.db.Query("SELECT Value FROM "+h.tableName+" WHERE Module = ? and name = ?", h.module, key)
	if err != nil {
		log.Println(err)
		return ""
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			log.Println(err)
			return ""
		}
		h.cache.Store(key, value)
		return value
	}

	if err := rows.Err(); err != nil {
		log.Println(err)
		return ""
	}
	return ""
}

func (h *MySQLHandler) Set(key string, value string) error {
	// 检查记录是否存在
	var exists bool
	err := h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM "+h.tableName+" WHERE Module = ? AND name = ?)", h.module, key).Scan(&exists)
	if err != nil {
		log.Println(err)
		return err
	}

	if exists {
		// 如果存在，则更新 Value
		_, err := h.db.Exec("UPDATE "+h.tableName+"  SET Value = ? WHERE Module = ? AND name = ?", value, h.module, key)
		if err != nil {
			log.Println(err)
			return err
		}
	} else {
		// 如果不存在，则插入新记录
		_, err := h.db.Exec("INSERT INTO "+h.tableName+"  (Module, name, Value) VALUES (?, ?, ?)", h.module, key, value)
		if err != nil {
			log.Println(err)
			return err
		}
	}
	h.cache.Delete(key)
	return nil
}

func (h *MySQLHandler) Remove(key string) {
	defer func() {
		h.cache.Delete(key)
	}()
	// 执行删除操作
	result, err := h.db.Exec("DELETE FROM "+h.tableName+" WHERE Module = ? AND name = ?", h.module, key)
	if err != nil {
		log.Println(err)
	}

	// 检查是否有行被删除
	_, err = result.RowsAffected()
	if err != nil {
		log.Println(err)
	}
}

func (h *MySQLHandler) List() []Item {
	rows, err := h.db.Query("SELECT Module, name, Value FROM "+h.tableName+" where Module = ? ORDER BY name", h.module)
	if err != nil {
		log.Println(err)
		return nil
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.Module, &item.Key, &item.Value); err != nil {
			log.Println(err)
			return []Item{}
		}
		items = append(items, item)
	}
	return items
}

// 检查表是否存在
func checkTableExists(db *sql.DB, tableName string) bool {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)
	row := db.QueryRow(query)
	var name string
	err := row.Scan(&name)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("查询表失败: %v\n", err)
	}
	return name != ""
}

// 创建表
func createTable(db *sql.DB, tableName string) {
	createTableSQL := fmt.Sprintf(`
        CREATE TABLE %s (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            Value VARCHAR(4096) NOT NULL,
            Module VARCHAR(64) NOT NULL
        )`, tableName)

	_, err := db.Exec(createTableSQL)
	if err != nil {
		log.Printf("创建表失败: %v\n", err)
		panic(err)
	}
	createIndexSQL := fmt.Sprintf(`
        CREATE UNIQUE INDEX idx_easy_config_items_i1 ON %s (Module, name)`, tableName)

	_, err = db.Exec(createIndexSQL)
	if err != nil {
		log.Printf("创建索引失败: %v\n", err)
		panic(err)
	}
}

type PropertiesHandler struct {
	module    string
	configMap *map[string]string
	path      string
}

func (h *PropertiesHandler) Get(key string) string {
	return (*h.configMap)[h.module+"."+key]
}

func (h *PropertiesHandler) Set(key string, value string) error {
	// 先修改文件
	keyOfFile := h.module + "." + key
	if err := updateProperties(h.path, keyOfFile, value); err != nil {
		log.Println(err)
		return err
	}
	(*h.configMap)[h.module+"."+key] = value
	return nil
}

func (h *PropertiesHandler) Remove(key string) {
	keyOfFile := h.module + "." + key
	if err := removeKeyFromProperties(h.path, keyOfFile); err != nil {
		log.Println(err)
		return
	}
	delete(*h.configMap, keyOfFile)
}

func (h *PropertiesHandler) List() []Item {
	var items []Item
	for k, v := range *h.configMap {
		if strings.HasPrefix(k, h.module+".") {
			items = append(items, Item{
				Module: h.module,
				Key:    k[len(h.module)+1:],
				Value:  v,
			})
		}
	}
	return items
}

func removeKeyFromProperties(path string, key string) error {
	// 尝试打开文件，如果文件不存在则返回错误
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("文件不存在: %w", err)
		}
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 读取现有的内容
	scanner := bufio.NewScanner(file)
	var lines []string
	found := false

	for scanner.Scan() {
		line := scanner.Text()
		// 检查当前行是否为要删除的键
		if strings.HasPrefix(line, key+"=") {
			found = true // 找到要删除的键
			continue     // 跳过这一行
		}
		lines = append(lines, line) // 保留原有行
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}

	// 如果没有找到该键，则返回
	if !found {
		return nil
	}

	// 重新写入文件
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("清空文件失败: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("重置文件指针失败: %w", err)
	}

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("刷新写入缓冲区失败: %w", err)
	}

	return nil
}

// updateProperties 更新或添加键值对到 properties 文件
func updateProperties(path string, key string, value string) error {
	// 尝试打开文件，如果文件不存在则创建新文件
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("打开或创建文件失败: %w", err)
	}
	defer file.Close()

	// 读取现有的内容
	scanner := bufio.NewScanner(file)
	var lines []string
	found := false

	for scanner.Scan() {
		line := scanner.Text()
		// 检查当前行是否为要更新的键
		if strings.HasPrefix(line, key+"=") {
			lines = append(lines, key+"="+value) // 修改值
			found = true
		} else {
			lines = append(lines, line) // 保留原有行
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取文件失败: %w", err)
	}

	// 如果没有找到该键，则添加新键值对
	if !found {
		lines = append(lines, key+"="+value)
	}

	// 重新写入文件
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("清空文件失败: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("重置文件指针失败: %w", err)
	}

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("写入文件失败: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("刷新写入缓冲区失败: %w", err)
	}

	return nil
}

func resolveProperties(path string) (map[string]string, error) {
	file, _ := os.Open(path)
	defer file.Close()
	_, err := file.Stat()
	if err != nil {
		return nil, errors.New("file is not exist")
	}
	conf := make(map[string]string)
	br := bufio.NewReader(file)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			// 读取结束
			break
		}
		if err != nil {
			return nil, err
		}
		lineContent := string(line)
		prop := strings.TrimSpace(lineContent)
		if prop == "" {
			continue
		}
		key := prop[:strings.Index(prop, "=")]
		val := prop[strings.Index(prop, "=")+1:]
		conf[key] = val
	}
	return conf, nil
}
