# MQTT 高级功能测试指南

本文档总结了使用 `paho.mqtt.golang` 库实现的 MQTT 高级功能测试程序，包括保留消息、离线消息和遗嘱消息（墓碑程序）功能。

## 项目结构

```
test/
├── mqtt_producer.go                 # 基础生产者测试
├── mqtt_producer_test.go            # 生产者单元测试
├── README.md                        # 基础使用说明
├── consumer/
│   └── mqtt_consumer.go            # 基础消费者测试
├── advanced_features/
│   └── mqtt_advanced_features.go   # 综合高级功能测试
├── advanced/offline/
│   └── mqtt_offline.go             # 离线消息专项测试
└── will_test/
    └── will_demo.go                # 遗嘱消息专项测试
```

## 1. 保留消息（Retained Messages）

### 功能说明
保留消息是 MQTT 的一个特性，允许服务器为某个主题保留最后一条消息。当新的订阅者订阅该主题时，会立即收到这条保留消息。

### 实现代码
在 `test/advanced_features/mqtt_advanced_features.go` 中：

```go
// 发布保留消息
func publishRetainedMessage(client mqtt.Client) {
    message := "这是一条保留消息"
    // 关键参数：retained=true
    token := client.Publish("emqx/advanced/retained", 1, true, message)
    token.Wait()
}
```

### 测试方法
1. 运行综合测试程序发布保留消息：
   ```bash
   cd test/advanced_features
   go run mqtt_advanced_features.go
   ```

2. 在新的终端启动消费者：
   ```bash
   cd test/consumer
   go run mqtt_consumer.go
   ```
   （需要修改订阅主题为 `emqx/advanced/retained`）

3. 消费者会立即收到保留消息。

## 2. 离线消息（Offline Messages）

### 功能说明
通过设置 `CleanSession=false` 和合适的 QoS（1 或 2），当客户端断开连接时，服务器会为它保留消息，等客户端重新连接时再发送。

### 关键配置
```go
// 创建持久化会话客户端
opts.SetCleanSession(false)  // 关键设置
opts.SetStore(mqtt.NewMemoryStore())  // 消息存储
```

### 测试程序
专项测试程序：`test/advanced/offline/mqtt_offline.go`

### 测试步骤
1. 启动离线消息测试消费者：
   ```bash
   cd test/advanced/offline
   go run mqtt_offline.go
   ```

2. 发送控制命令让客户端离线：
   ```bash
   mosquitto_pub -h broker.emqx.io -t 'emqx/offline/control' \
     -m 'go_offline' -u emqx -P public
   ```

3. 在客户端离线时发布消息：
   ```bash
   mosquitto_pub -h broker.emqx.io -t 'emqx/offline/test' \
     -m '离线测试消息' -q 1 -u emqx -P public
   ```

4. 等待客户端重新连接，观察是否收到离线期间的消息。

## 3. 遗嘱消息/墓碑程序（Last Will and Testament, LWT）

### 功能说明
遗嘱消息是客户端在连接时指定的一条消息。如果客户端异常断开（没有发送 DISCONNECT 包），服务器会自动发布这条遗嘱消息到指定主题。

### 实现代码
```go
// 设置遗嘱消息
willMessage := "客户端异常断开连接"
opts.SetWill("emqx/will/test", willMessage, 1, false)
```

### 测试程序
专项测试程序：`test/will_test/will_demo.go`

### 测试用例

#### 用例1：正常断开（不应触发遗嘱消息）
```go
// 正常断开连接
client.Disconnect(250)  // 发送DISCONNECT包
```

#### 用例2：异常断开（应触发遗嘱消息）
```go
// 模拟异常断开（不发送DISCONNECT包）
// 实际测试中：强制结束进程或断开网络
```

#### 用例3：多客户端测试
创建多个带有遗嘱消息的客户端，模拟部分客户端异常断开。

### 测试方法
1. 启动监控程序：
   ```bash
   cd test/will_test
   go run will_demo.go
   ```

2. 创建测试客户端（使用 mosquitto_sub）：
   ```bash
   mosquitto_sub -h broker.emqx.io \
     -i 'test_client' -u emqx -P public \
     -t 'emqx/will/status' \
     --will-topic 'emqx/will/test' \
     --will-payload '客户端异常断开' \
     --will-qos 1 --will-retain false
   ```

3. 异常断开测试：
   - 强制结束进程：`kill -9 <pid>` 或 `Ctrl+\`
   - 断开网络连接
   - 等待 KeepAlive 超时（默认60秒）

4. 观察监控程序是否收到遗嘱消息。

## 综合测试程序

### `test/advanced_features/mqtt_advanced_features.go`
这是一个综合测试程序，演示所有三种高级功能：

1. **保留消息测试**：发布一条保留消息
2. **离线消息测试**：演示 CleanSession=false 的作用
3. **遗嘱消息测试**：设置并测试遗嘱消息功能

### 运行方法
```bash
cd test/advanced_features
go run mqtt_advanced_features.go
```

### 输出说明
程序会显示三个测试的说明和预期结果，然后保持运行状态，允许用户进行实际测试。

## 关键概念总结

### 1. QoS（服务质量）等级
- **QoS 0**：最多一次，不保证送达
- **QoS 1**：至少一次，保证送达但可能重复
- **QoS 2**：恰好一次，保证送达且不重复

### 2. CleanSession 参数
- **true**：清理会话，客户端离线时丢失所有状态
- **false**：持久化会话，服务器保留客户端状态和未送达消息

### 3. 消息保留（Retained）
- **true**：服务器保留该主题的最后一条消息
- **false**：不保留消息（默认）

### 4. 遗嘱消息触发条件
- 客户端异常断开（未发送 DISCONNECT）
- KeepAlive 超时
- 网络连接丢失

## 实际应用场景

### 1. 物联网设备监控
```go
// 设备连接时设置遗嘱消息
willMsg := "设备异常离线，请检查"
opts.SetWill("iot/device/status", willMsg, 1, false)
```

### 2. 实时聊天应用
```go
// 用户异常退出时通知其他用户
willMsg := fmt.Sprintf("用户 %s 异常退出", username)
opts.SetWill("chat/user/offline", willMsg, 1, false)
```

### 3. 系统服务监控
```go
// 服务异常停止时发送警报
willMsg := "服务异常停止，需要重启"
opts.SetWill("monitor/service/status", willMsg, 2, true)
```

## 故障排除

### 常见问题

1. **遗嘱消息未触发**
   - 检查是否正常断开（发送了 DISCONNECT）
   - 确认 KeepAlive 时间设置
   - 检查网络连接状态

2. **离线消息未送达**
   - 确认 CleanSession=false
   - 检查 QoS 等级（需要 1 或 2）
   - 验证客户端 ID 是否相同

3. **保留消息未收到**
   - 确认发布时设置了 retained=true
   - 检查主题名称是否完全匹配
   - 确认服务器支持保留消息

### 调试建议
```go
// 启用调试日志
import "log"
import "os"

mqtt.DEBUG = log.New(os.Stdout, "[MQTT DEBUG] ", 0)
mqtt.ERROR = log.New(os.Stdout, "[MQTT ERROR] ", 0)
```

## 性能考虑

1. **内存使用**：持久化会话会占用服务器内存
2. **网络流量**：QoS 2 需要四次握手，流量较大
3. **延迟**：KeepAlive 时间影响异常检测速度
4. **存储**：大量保留消息会增加存储需求

## 安全建议

1. **使用 TLS**：生产环境建议使用加密连接
2. **认证授权**：配置用户名密码和 ACL
3. **客户端 ID**：使用唯一且不易猜测的客户端 ID
4. **遗嘱消息**：避免在遗嘱消息中泄露敏感信息

## 扩展阅读

1. [MQTT 协议规范](https://mqtt.org/)
2. [paho.mqtt.golang 文档](https://github.com/eclipse/paho.mqtt.golang)
3. [EMQX 文档](https://www.emqx.io/docs/zh/)
4. [MQTT 5.0 新特性](https://www.emqx.com/zh/blog/introduction-to-mqtt-5)

## 总结

本文档提供了完整的 MQTT 高级功能测试方案，包括：

- ✅ 保留消息测试：确保新订阅者能立即收到最新状态
- ✅ 离线消息测试：保证消息在客户端离线时不丢失
- ✅ 遗嘱消息测试：及时检测客户端异常断开

所有测试程序都已实现并验证，可以直接运行测试。这些功能是构建可靠 MQTT 应用的基础，特别适用于物联网、实时通信和系统监控等场景。

---

*最后更新: 2026-01-08*  
*测试服务器: broker.emqx.io:1883*  
*用户名: emqx*  
*密码: public*