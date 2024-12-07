# easy-config

这是一个简易的配置工具，当你的程序需要纯字符串配置时，可以方便地进行设置和获取值。

# 使用方法

`go get -u github.com/kiririx/easy-config`

## 存储在MySQL

```go
h := ec.Initialize(ec.NewMySQLStorage("127.0.0.1", 3306, "root", "password", "database"), "main")
h.Set("word.en", "this is an apple")
h.Set("word.zh", "这是一个苹果")
t.Log(h.Get("word.zh"))
```

## 存储在properties文件
```go
h := ec.Initialize(ec.NewPropertiesStorage("D:\\easy-config.properties"), "main")
h.Set("word.en", "this is an apple")
t.Log(h.Get("word.en"))
h.Remove("word.en")
t.Log(h.Get("word.en"))
```

# 开发计划
下个版本加入并发安全控制