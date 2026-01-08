# MQTT 测试工具集（生产者 + 消费者）

这是一个基于 Go 语言的 MQTT 测试工具集，专为 go-zero RPC 服务项目设计。包含完整的生产者和消费者实现，可以连接到 EMQX 公共 MQTT 服务器，进行消息的发布和消费测试。

## 功能特性

### 生产者功能
- ✅ 连接到 EMQX 公共 MQTT 服务器
- ✅ 发布测试消息到指定主题
- ✅ 优雅退出处理（Ctrl+C）
- ✅ 心跳保持连接
- ✅ 完整的错误处理和日志记录

### 消费者功能  
- ✅ 订阅主题并接收消息
- ✅ 消息统计和速率计算
- ✅ 自动重连机制
- ✅ 持久化会话（避免消息丢失）
- ✅ 优雅退出和统计信息显示

### 测试覆盖
- ✅ 单元测试覆盖核心逻辑
- ✅ 代码编译验证
- ✅ 配置常量测试

## 快速开始

### 1. 安装依赖

```bash
go get github.com/eclipse/paho.mqtt.golang
```

### 2. 运行测试生产者

```bash
cd test
go run mqtt_producer.go
```

### 3. 运行测试消费者（消费消息）

```bash
cd test/consumer
go run mqtt_consumer.go
```

### 4. 测试生产消费流程

1. **首先启动消费者**（在一个终端）：
   ```bash
   cd test/consumer
   go run mqtt_consumer.go
   ```

2. **然后启动生产者**（在另一个终端）：
   ```bash
   cd test
   go run mqtt_producer.go
   ```

3. **观察消费者终端**，应该能看到类似以下输出：
   ```
   📨 收到消息 #1
       主题: emqx/gozero/test
       内容: 测试消息 #1 - 时间: 2026-01-08 14:38:46 - 来自: go_mqtt_producer_1767854326
       QoS: 0
       时间: 2026-01-08 14:38:46
   ```

## 如何消费消息

### 方法一：使用内置消费者程序

最简单的消费方式就是运行我们提供的消费者程序：

```bash
cd test/consumer
go run mqtt_consumer.go
```

消费者会自动：
1. 连接到 `broker.emqx.io:1883`
2. 订阅主题 `emqx/gozero/test`
3. 显示所有收到的消息
4. 统计消息数量和接收速率
5. 支持优雅退出（Ctrl+C）

### 方法二：集成到现有代码中

如果你需要在现有 go-zero 服务中消费 MQTT 消息，可以参考以下代码片段：

```go
package main

import (
    "fmt"
    "time"
    mqtt "github.com/eclipse/paho.mqtt.golang"
)

func setupMQTTConsumer() mqtt.Client {
    opts := mqtt.NewClientOptions()
    opts.AddBroker("tcp://broker.emqx.io:1883")
    opts.SetClientID("your_client_id")
    opts.SetUsername("emqx")
    opts.SetPassword("public")
    
    // 设置消息处理回调
    opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
        fmt.Printf("收到消息: %s\n", msg.Payload())
        // 在这里处理业务逻辑
    })
    
    client := mqtt.NewClient(opts)
    if token := client.Connect(); token.Wait() && token.Error() != nil {
        panic(token.Error())
    }
    
    // 订阅主题
    token := client.Subscribe("emqx/gozero/test", 1, nil)
    token.Wait()
    
    return client
}
```

### 方法三：使用 MQTT 客户端工具

你也可以使用第三方 MQTT 客户端工具来消费消息，例如：

1. **MQTTX**（图形界面工具）：
   - 下载地址：https://mqttx.app/
   - 配置连接：broker.emqx.io:1883，用户名 emqx，密码 public
   - 订阅主题：emqx/gozero/test

2. **mosquitto_sub**（命令行工具）：
   ```bash
   mosquitto_sub -h broker.emqx.io -p 1883 -u emqx -P public -t "emqx/gozero/test"
   ```

## 配置选项

### 生产者配置（mqtt_producer.go）
| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `broker` | `broker.emqx.io` | MQTT 服务器地址 |
| `port` | `1883` | MQTT 服务器端口 |
| `username` | `emqx` | 用户名 |
| `password` | `public` | 密码 |
| `messageCount` | `10` | 测试消息数量 |
| `publishTopic` | `emqx/gozero/test` | 发布主题 |

### 消费者配置（consumer/mqtt_consumer.go）
| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `broker` | `broker.emqx.io` | MQTT 服务器地址 |
| `port` | `1883` | MQTT 服务器端口 |
| `username` | `emqx` | 用户名 |
| `password` | `public` | 密码 |
| `subscribeTopic` | `emqx/gozero/test` | 订阅主题 |
| `QoS` | `1` | 服务质量等级（至少送达一次） |

## 高级用法

### 使用 TLS/SSL 连接

如果需要使用加密连接，可以修改配置：

```go
const (
    broker   = "broker.emqx.io"
    port     = 8883  // SSL端口
)
```

并在客户端选项中修改连接URL：

```go
opts.AddBroker(fmt.Sprintf("ssl://%s:%d", broker, port))
```

### 修改消息处理逻辑

在消费者中，可以修改 `consumerMessageHandler` 函数来实现自定义业务逻辑：

```go
var consumerMessageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
    // 解析消息内容
    payload := string(msg.Payload())
    
    // 根据消息内容执行不同操作
    if strings.Contains(payload, "alert") {
        fmt.Println("收到警报消息:", payload)
        // 发送通知等...
    } else if strings.Contains(payload, "data") {
        fmt.Println("收到数据消息:", payload)
        // 处理数据等...
    }
    
    // 确认消息已处理
    msg.Ack()
}
```

### 订阅多个主题

```go
// 订阅多个主题，每个主题可以有不同的QoS
topics := map[string]byte{
    "emqx/gozero/test":     1,
    "emqx/gozero/status":   0,
    "emqx/gozero/control":  2,
}

token := client.SubscribeMultiple(topics, nil)
token.Wait()
```

### 运行单元测试

```bash
cd test
go test -v
```

## 项目结构

```
test/
├── mqtt_producer.go          # 生产者主程序
├── mqtt_producer_test.go     # 生产者单元测试
├── README.md                 # 本说明文件
├── consumer/                 # 消费者目录
│   └── mqtt_consumer.go     # 消费者主程序
└── (编译后的可执行文件)
```

## 故障排除

### 常见问题

1. **连接失败**
   - 检查网络连接
   - 验证 `broker.emqx.io` 是否可访问
   - 确认防火墙设置允许出站连接

2. **认证失败**
   - 确认用户名和密码正确
   - 检查客户端 ID 是否唯一

3. **消息未送达**
   - 检查主题名称是否正确
   - 确认消费者已正确订阅主题
   - 检查 QoS 设置是否匹配

4. **消费者收不到消息**
   - 确保消费者在生产者之前启动（或使用持久化会话）
   - 检查主题名称是否完全一致（包括大小写）
   - 确认网络连接正常

### 调试模式

启用 MQTT 调试日志：

```go
import "log"
import "os"

// 在 main 函数开始处添加
mqtt.DEBUG = log.New(os.Stdout, "[MQTT DEBUG] ", 0)
mqtt.ERROR = log.New(os.Stdout, "[MQTT ERROR] ", 0)
```

## 与 go-zero 项目集成

### 方案一：独立运行
将生产者和消费者作为独立程序运行，不修改现有 RPC 服务代码。

### 方案二：集成到服务中
如果需要将 MQTT 功能集成到 go-zero RPC 服务中，可以参考以下步骤：

1. 在 `internal/svc/servicecontext.go` 中添加 MQTT 客户端
2. 在业务逻辑中使用 MQTT 客户端发布/消费消息
3. 在配置文件中添加 MQTT 相关配置

详细集成方案请参考 `../MQTT_Golang_使用指南.md` 文档。

## 性能特性

- 连接超时设置（30秒）
- 心跳保持（30秒间隔）
- 自动重连机制
- 优雅退出处理
- 消息接收速率统计
- 持久化会话支持

## 许可证

本项目基于 MIT 许可证开源。

## 参考资料

- [paho.mqtt.golang 官方文档](https://github.com/eclipse/paho.mqtt.golang)
- [EMQX 公共 MQTT 服务器](https://www.emqx.io/docs/zh/)
- [go-zero 官方文档](https://go-zero.dev/)
- [MQTT 协议规范](https://mqtt.org/)

---

*最后更新: 2026-01-08*