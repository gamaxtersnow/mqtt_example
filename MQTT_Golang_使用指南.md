# 在 Golang 中使用 MQTT - 测试生产者实现指南

## 概述

本文档介绍如何在基于 go-zero 的 RPC 服务项目中集成 MQTT 客户端，实现一个测试生产者。我们将使用 `paho.mqtt.golang` 客户端库连接到 EMQX 公共 MQTT 服务器。

## 项目背景

当前项目是一个 go-zero 实现的 RPC 服务，包含以下主要组件：
- `emqx.go` - 主服务入口
- `emqx.proto` - gRPC 服务定义
- `emqxclient/` - 生成的客户端代码
- `internal/` - 内部业务逻辑

## MQTT 基础知识

### 什么是 MQTT？
MQTT（Message Queuing Telemetry Transport）是一种基于发布/订阅模式的轻量级物联网消息传输协议。它具有以下特点：
- 极少的代码和带宽消耗
- 为联网设备提供实时可靠的消息服务
- 广泛应用于物联网、移动互联网、智能硬件等行业

### 核心概念
- **Broker**: MQTT 消息代理服务器
- **Topic**: 消息主题，用于消息分类
- **Publisher**: 消息发布者（生产者）
- **Subscriber**: 消息订阅者（消费者）
- **QoS**: 服务质量等级（0, 1, 2）

## 环境准备

### 1. 安装依赖
```bash
go get github.com/eclipse/paho.mqtt.golang
```

### 2. 公共 MQTT 服务器
我们将使用 EMQX 提供的免费公共 MQTT 服务器：
- **Broker**: `broker.emqx.io`
- **TCP Port**: `1883`
- **WebSocket Port**: `8083`
- **用户名**: `emqx`
- **密码**: `public`

## MQTT 客户端实现

### 1. 基本连接配置

```go
package main

import (
    "fmt"
    "time"
    mqtt "github.com/eclipse/paho.mqtt.golang"
)

// 消息处理回调
var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
    fmt.Printf("收到消息: %s 来自主题: %s\n", msg.Payload(), msg.Topic())
}

// 连接成功回调
var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
    fmt.Println("连接成功")
}

// 连接丢失回调
var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
    fmt.Printf("连接丢失: %v", err)
}

func createMQTTClient() mqtt.Client {
    broker := "broker.emqx.io"
    port := 1883
    
    opts := mqtt.NewClientOptions()
    opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
    opts.SetClientID("go_mqtt_producer")
    opts.SetUsername("emqx")
    opts.SetPassword("public")
    opts.SetDefaultPublishHandler(messagePubHandler)
    opts.OnConnect = connectHandler
    opts.OnConnectionLost = connectLostHandler
    
    client := mqtt.NewClient(opts)
    return client
}
```

### 2. 连接 MQTT 服务器

```go
func connectMQTT(client mqtt.Client) error {
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        return token.Error()
    }
    return nil
}
```

### 3. 发布消息（生产者功能）

```go
func publishMessages(client mqtt.Client, topic string, messageCount int) {
    for i := 0; i < messageCount; i++ {
        text := fmt.Sprintf("测试消息 %d - 时间: %s", i, time.Now().Format("2006-01-02 15:04:05"))
        token := client.Publish(topic, 0, false, text)
        token.Wait()
        fmt.Printf("已发布消息: %s\n", text)
        time.Sleep(1 * time.Second)
    }
}
```

### 4. 订阅主题（可选，用于测试）

```go
func subscribeTopic(client mqtt.Client, topic string) {
    token := client.Subscribe(topic, 1, nil)
    token.Wait()
    fmt.Printf("已订阅主题: %s\n", topic)
}
```

## 集成到 go-zero 项目

### 方案一：独立测试生产者

创建一个独立的测试程序，不修改现有 RPC 服务代码：

```go
// mqtt_producer/main.go
package main

import (
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "time"
    mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
    // 创建 MQTT 客户端
    client := createMQTTClient()
    
    // 连接服务器
    if err := connectMQTT(client); err != nil {
        panic(err)
    }
    
    // 设置优雅退出
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    
    go func() {
        <-sigChan
        fmt.Println("\n收到终止信号，正在断开连接...")
        client.Disconnect(250)
        os.Exit(0)
    }()
    
    // 发布测试消息
    topic := "emqx/test/producer"
    fmt.Printf("开始向主题 '%s' 发布消息...\n", topic)
    publishMessages(client, topic, 10)
    
    // 保持连接
    fmt.Println("消息发布完成，按 Ctrl+C 退出")
    select {}
}
```

### 方案二：作为 RPC 服务的扩展

如果需要将 MQTT 功能集成到现有的 RPC 服务中，可以在 `internal/svc/servicecontext.go` 中添加 MQTT 客户端：

```go
// internal/svc/servicecontext.go 扩展
type ServiceContext struct {
    Config config.Config
    MQTTClient mqtt.Client
}

func NewServiceContext(c config.Config) *ServiceContext {
    // 创建 MQTT 客户端
    client := createMQTTClient()
    if err := connectMQTT(client); err != nil {
        logx.Error("MQTT 连接失败: %v", err)
        // 可以根据需要决定是否 panic
    }
    
    return &ServiceContext{
        Config: c,
        MQTTClient: client,
    }
}
```

## 测试生产者实现

### 完整测试代码

创建一个完整的测试生产者程序：

```go
// test/mqtt_producer_test.go
package main

import (
    "fmt"
    "log"
    "time"
    mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
    // 配置选项
    broker := "broker.emqx.io"
    port := 1883
    clientID := "go_mqtt_producer_test"
    topic := "emqx/gozero/test"
    
    // 创建客户端选项
    opts := mqtt.NewClientOptions()
    opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
    opts.SetClientID(clientID)
    opts.SetUsername("emqx")
    opts.SetPassword("public")
    
    // 设置回调
    opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
        fmt.Printf("收到消息: %s\n", msg.Payload())
    })
    
    opts.OnConnect = func(client mqtt.Client) {
        fmt.Println("连接到 MQTT 服务器成功")
    }
    
    opts.OnConnectionLost = func(client mqtt.Client, err error) {
        fmt.Printf("连接丢失: %v\n", err)
    }
    
    // 创建并连接客户端
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        log.Fatal(token.Error())
    }
    
    // 发布测试消息
    for i := 1; i <= 5; i++ {
        text := fmt.Sprintf("Hello EMQX - 消息 #%d", i)
        token := client.Publish(topic, 0, false, text)
        token.Wait()
        fmt.Printf("已发布: %s\n", text)
        time.Sleep(2 * time.Second)
    }
    
    // 断开连接
    client.Disconnect(250)
    fmt.Println("测试完成，已断开连接")
}
```

### 运行测试

1. 创建测试目录和文件：
```bash
mkdir -p test
cd test
```

2. 初始化 go module（如果需要）：
```bash
go mod init mqtt_test
go get github.com/eclipse/paho.mqtt.golang
```

3. 运行测试：
```bash
go run mqtt_producer_test.go
```

## 高级功能

### TLS/SSL 连接

如果需要使用加密连接（端口 8883）：

```go
func createTLSClient() mqtt.Client {
    broker := "broker.emqx.io"
    port := 8883
    
    opts := mqtt.NewClientOptions()
    opts.AddBroker(fmt.Sprintf("ssl://%s:%d", broker, port))
    opts.SetClientID("go_mqtt_tls_client")
    opts.SetUsername("emqx")
    opts.SetPassword("public")
    
    // 配置 TLS（如果需要自定义证书）
    // tlsConfig := &tls.Config{
    //     InsecureSkipVerify: true, // 仅测试使用
    // }
    // opts.SetTLSConfig(tlsConfig)
    
    return mqtt.NewClient(opts)
}
```

### 持久化会话

```go
opts.SetCleanSession(false) // 启用持久化会话
opts.SetStore(mqtt.NewFileStore("/tmp/mqtt-store")) // 设置存储路径
```

### 遗嘱消息

```go
opts.SetWill("emqx/client/status", "offline", 1, true)
```

## 故障排除

### 常见问题

1. **连接失败**
   - 检查网络连接
   - 验证 broker 地址和端口
   - 确认防火墙设置

2. **认证失败**
   - 检查用户名和密码
   - 确认客户端 ID 唯一性

3. **消息未送达**
   - 检查主题名称
   - 确认 QoS 设置
   - 查看订阅者是否在线

### 调试建议

```go
// 启用调试日志
mqtt.DEBUG = log.New(os.Stdout, "[DEBUG] ", 0)
mqtt.ERROR = log.New(os.Stdout, "[ERROR] ", 0)
```

## 性能优化

1. **连接池管理**：对于高频发布，考虑使用连接池
2. **批量发布**：合并小消息为批量消息
3. **QoS 选择**：根据业务需求选择合适的 QoS 等级
4. **心跳间隔**：调整 KeepAlive 参数优化网络使用

## 与现有 RPC 服务集成建议

考虑到当前项目是 go-zero RPC 服务，建议：

1. **独立部署**：将 MQTT 生产者作为独立服务部署，通过消息队列与 RPC 服务通信
2. **Sidecar 模式**：将 MQTT 客户端作为 sidecar 容器与 RPC 服务一起部署
3. **微服务集成**：将 MQTT 功能封装为单独微服务，通过 gRPC 与其他服务通信

## 总结

本文档介绍了如何在 Golang 项目中集成 MQTT 客户端实现测试生产者。通过使用 `paho.mqtt.golang` 库，我们可以轻松连接到 EMQX 公共服务器，实现消息的发布和订阅功能。

对于当前的 go-zero RPC 项目，建议先实现独立的测试生产者验证功能，再根据实际需求决定是否集成到主服务中。

## 参考资料

1. [paho.mqtt.golang 官方文档](https://github.com/eclipse/paho.mqtt.golang)
2. [EMQX 官方文档](https://www.emqx.io/docs/zh/)
3. [MQTT 协议规范](https://mqtt.org/)
4. [go-zero 官方文档](https://go-zero.dev/)

---

*最后更新: 2026-01-08*