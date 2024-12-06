package ec

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

type StorageType int

var (
	MySQL      StorageType = 0
	Properties StorageType = 1
)

type Storage interface {
	getStorage() StorageType
	init(module string) Handler
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

//var db *sql.DB

func (receiver *MySQLStorage) init(module string) Handler {
	// 数据库连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", receiver.user, receiver.pass, receiver.host, receiver.port, receiver.database)

	// 连接数据库
	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	// 测试数据库连接
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("[easy-config] Successfully connected to the database!")
	// 检查表是否存在
	tableName := receiver.table
	if !checkTableExists(db, tableName) {
		createTable(db, tableName)
	}
	return &MySQLHandler{
		module: module,
		db:     db,
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
	return storage.init(module)
}

type Handler interface {
	Get(key string) string
	Set(key string, value string)
}

type MySQLHandler struct {
	module    string
	db        *sql.DB
	tableName string
}

func (h *MySQLHandler) Get(key string) string {
	// 执行查询
	rows, err := h.db.Query("SELECT value FROM "+h.tableName+" WHERE module = ? and name = ?", h.module, key)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			log.Fatal(err)
		}
		return value
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return ""
}

func (h *MySQLHandler) Set(key string, value string) {
	// 检查记录是否存在
	var exists bool
	err := h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM "+h.tableName+" WHERE module = ? AND name = ?)", h.module, key).Scan(&exists)
	if err != nil {
		log.Fatal(err)
	}

	if exists {
		// 如果存在，则更新 value
		_, err := h.db.Exec("UPDATE "+h.tableName+"  SET value = ? WHERE module = ? AND name = ?", value, h.module, key)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		// 如果不存在，则插入新记录
		_, err := h.db.Exec("INSERT INTO "+h.tableName+"  (module, name, value) VALUES (?, ?, ?)", h.module, key, value)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Get(key string) {

}

func Set(key string, value string) {

}

// 检查表是否存在
func checkTableExists(db *sql.DB, tableName string) bool {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)
	row := db.QueryRow(query)
	var name string
	err := row.Scan(&name)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalf("查询表失败: %v", err)
	}
	return name != ""
}

// 创建表
func createTable(db *sql.DB, tableName string) {
	createTableSQL := fmt.Sprintf(`
        CREATE TABLE %s (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value VARCHAR(4096) NOT NULL,
            module VARCHAR(64) NOT NULL
        )`, tableName)

	_, err := db.Exec(createTableSQL)
	if err != nil {
		log.Fatalf("创建表失败: %v", err)
	}
}
