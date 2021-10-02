## 一致性哈希

### 安装
```
go get -u github.com/junhaideng/consistent
```

### 使用
```go
c := consistent.New()
ips := []string{"192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4"}

for _, ip := range ips {
  c.Add(ip)
}

fmt.Println("ip: ", c.Get("/hello.txt"))
```