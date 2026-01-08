package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// MQTT配置
const (
	broker   = "broker.emqx.io"
	port     = 1883
	username = "emqx"
	password = "public"
)

// 测试主题
const (
	retainedTopic = "emqx/advanced/retained" // 保留消息主题
	offlineTopic  = "emqx/advanced/offline"  // 离线消息主题
	willTopic     = "emqx/advanced/will"     // 遗嘱消息主题
	statusTopic   = "emqx/advanced/status"   // 状态主题
)

// 全局变量
var (
	clientID      = "mqtt_advanced_" + fmt.Sprintf("%d", time.Now().Unix())
	receivedCount = 0
	startTime     time.Time
)

// 消息处理回调
var messageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	receivedCount++

	fmt.Printf("\n📨 收到消息 #%d\n", receivedCount)
	fmt.Printf("   主题: %s\n", msg.Topic())
	fmt.Printf("   内容: %s\n", msg.Payload())
	fmt.Printf("   QoS: %d\n", msg.Qos())
	fmt.Printf("   保留: %v\n", msg.Retained())
	fmt.Printf("   时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	// 特殊标记保留消息
	if msg.Retained() {
		fmt.Println("   ⚡ 这是一条保留消息！")
	}

	// 特殊标记遗嘱消息
	if msg.Topic() == willTopic {
		fmt.Println("   ⚰️  这是一条遗嘱消息！")
	}

	msg.Ack()
}

// 连接成功回调
var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("✅ 成功连接到 MQTT 服务器")
	fmt.Printf("   客户端ID: %s\n", clientID)
	fmt.Printf("   服务器: %s:%d\n", broker, port)
}

// 连接丢失回调
var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("❌ 连接丢失: %v\n", err)
}

// 创建带有高级功能的MQTT客户端
func createAdvancedMQTTClient(enableWill bool, cleanSession bool) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)

	opts.SetDefaultPublishHandler(messageHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	// 设置连接参数
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetCleanSession(cleanSession) // 控制离线消息行为
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)

	// 设置遗嘱消息（Last Will and Testament）
	if enableWill {
		willMessage := fmt.Sprintf("客户端 %s 异常断开连接 - 时间: %s",
			clientID, time.Now().Format("2006-01-02 15:04:05"))
		opts.SetWill(willTopic, willMessage, 1, false)
		fmt.Println("✅ 已启用遗嘱消息功能")
	}

	// 设置消息存储（用于离线消息）
	opts.SetStore(mqtt.NewMemoryStore())

	return mqtt.NewClient(opts)
}

// 发布保留消息
func publishRetainedMessage(client mqtt.Client) {
	message := fmt.Sprintf("这是一条保留消息 - 发布时间: %s - 客户端: %s",
		time.Now().Format("2006-01-02 15:04:05"), clientID)

	// 发布保留消息（retained=true）
	token := client.Publish(retainedTopic, 1, true, message)
	token.Wait()

	if token.Error() != nil {
		log.Printf("发布保留消息失败: %v", token.Error())
		return
	}

	fmt.Printf("\n✅ 已发布保留消息到主题 '%s':\n", retainedTopic)
	fmt.Printf("   内容: %s\n", message)
	fmt.Println("   注意: 这条消息会被服务器保留，新的订阅者会立即收到")
}

// 发布普通消息（用于离线消息测试）
func publishOfflineMessage(client mqtt.Client, messageNum int) {
	message := fmt.Sprintf("离线测试消息 #%d - 时间: %s",
		messageNum, time.Now().Format("2006-01-02 15:04:05"))

	// 发布QoS 1消息（确保送达）
	token := client.Publish(offlineTopic, 1, false, message)
	token.Wait()

	if token.Error() != nil {
		log.Printf("发布离线测试消息失败: %v", token.Error())
		return
	}

	fmt.Printf("✅ 已发布离线测试消息 #%d\n", messageNum)
}

// 订阅所有测试主题
func subscribeAllTopics(client mqtt.Client) error {
	fmt.Println("\n正在订阅测试主题...")

	// 订阅多个主题
	topics := map[string]byte{
		retainedTopic: 1, // QoS 1
		offlineTopic:  1, // QoS 1
		willTopic:     1, // QoS 1
		statusTopic:   0, // QoS 0
	}

	for topic, qos := range topics {
		fmt.Printf("   - %s (QoS: %d)\n", topic, qos)
	}

	token := client.SubscribeMultiple(topics, nil)
	token.Wait()

	if token.Error() != nil {
		return token.Error()
	}

	fmt.Println("✅ 所有主题订阅成功")
	return nil
}

// 测试保留消息功能
func testRetainedMessage(client mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🧪 测试 1: 保留消息功能")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("步骤 1: 发布一条保留消息")
	publishRetainedMessage(client)

	fmt.Println("\n步骤 2: 等待新的订阅者连接并接收保留消息")
	fmt.Println("   （新启动的消费者会立即收到这条保留消息）")

	time.Sleep(2 * time.Second)
}

// 测试离线消息功能
func testOfflineMessage(client mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🧪 测试 2: 离线消息功能")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("步骤 1: 发布几条QoS 1消息（此时消费者在线）")
	for i := 1; i <= 3; i++ {
		publishOfflineMessage(client, i)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\n步骤 2: 模拟消费者离线")
	fmt.Println("   （关闭消费者客户端，然后发布更多消息）")

	fmt.Println("\n步骤 3: 消费者重新连接后应该收到离线期间的消息")
	fmt.Println("   （需要 CleanSession=false 和合适的QoS）")

	time.Sleep(2 * time.Second)
}

// 测试遗嘱消息功能
func testWillMessage(client mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🧪 测试 3: 遗嘱消息（Last Will）功能")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("步骤 1: 客户端已设置遗嘱消息")
	fmt.Printf("   遗嘱主题: %s\n", willTopic)
	fmt.Println("   遗嘱内容: 客户端异常断开连接通知")

	fmt.Println("\n步骤 2: 模拟客户端异常断开")
	fmt.Println("   （强制关闭客户端连接，不发送DISCONNECT包）")

	fmt.Println("\n步骤 3: 服务器会发布遗嘱消息到指定主题")
	fmt.Println("   （其他订阅者会收到客户端异常断开通知）")

	// 发布状态消息
	statusMsg := fmt.Sprintf("客户端 %s 正常运行中", clientID)
	token := client.Publish(statusTopic, 0, false, statusMsg)
	token.Wait()

	time.Sleep(2 * time.Second)
}

// 模拟异常断开（用于测试遗嘱消息）
func simulateAbnormalDisconnect(client mqtt.Client) {
	fmt.Println("\n⚠️  模拟异常断开连接...")
	fmt.Println("   注意: 不发送DISCONNECT包，直接终止连接")
	fmt.Println("   服务器将在KeepAlive超时后发布遗嘱消息")

	// 在实际测试中，这里应该直接关闭网络连接
	// 为了演示，我们只是记录这个操作
	fmt.Println("   （在实际测试中，需要强制关闭客户端连接）")
}

// 优雅退出处理
func setupGracefulShutdown(client mqtt.Client) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n\n收到信号: %v，正在优雅退出...\n", sig)

		// 发布正常离线状态（不是遗嘱消息）
		offlineMsg := fmt.Sprintf("客户端 %s 正常离线 - 时间: %s",
			clientID, time.Now().Format("2006-01-02 15:04:05"))
		client.Publish(statusTopic, 1, false, offlineMsg)
		time.Sleep(100 * time.Millisecond)

		// 显示统计信息
		fmt.Printf("\n📊 测试统计:\n")
		fmt.Printf("   总共收到消息: %d 条\n", receivedCount)
		fmt.Printf("   运行时间: %s\n", time.Since(startTime).Round(time.Second))

		// 正常断开连接（不会触发遗嘱消息）
		client.Disconnect(250)
		fmt.Println("✅ MQTT连接已正常断开")

		os.Exit(0)
	}()
}

// 显示使用信息
func showUsageInfo() {
	separator := strings.Repeat("=", 60)
	fmt.Println(separator)
	fmt.Println("MQTT 高级功能测试程序")
	fmt.Println(separator)
	fmt.Printf("服务器: %s:%d\n", broker, port)
	fmt.Printf("客户端ID: %s\n", clientID)
	fmt.Println(separator)
	fmt.Println("测试功能:")
	fmt.Println("   1. 保留消息 (Retained Messages)")
	fmt.Println("   2. 离线消息 (Offline Messages)")
	fmt.Println("   3. 遗嘱消息 (Last Will and Testament)")
	fmt.Println(separator)
	fmt.Println("按 Ctrl+C 退出程序")
	fmt.Println(separator)
	fmt.Println()
}

func main() {
	startTime = time.Now()

	showUsageInfo()

	// 创建带有高级功能的客户端
	// CleanSession=false 启用离线消息功能
	// enableWill=true 启用遗嘱消息功能
	client := createAdvancedMQTTClient(true, false)

	setupGracefulShutdown(client)

	// 连接MQTT服务器
	fmt.Println("正在连接 MQTT 服务器...")
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ 连接MQTT服务器失败: %v", token.Error())
	}

	// 订阅所有测试主题
	if err := subscribeAllTopics(client); err != nil {
		log.Fatalf("❌ 订阅主题失败: %v", err)
	}

	// 等待连接稳定
	time.Sleep(2 * time.Second)

	// 运行所有测试
	testRetainedMessage(client)
	testOfflineMessage(client)
	testWillMessage(client)

	// 显示测试说明
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("✅ 所有测试说明已完成")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\n下一步操作建议:")
	fmt.Println("1. 保留消息测试:")
	fmt.Println("   启动一个新的消费者，订阅主题 'emqx/advanced/retained'")
	fmt.Println("   会立即收到我们刚才发布的保留消息")

	fmt.Println("\n2. 离线消息测试:")
	fmt.Println("   a. 启动一个消费者（CleanSession=false）")
	fmt.Println("   b. 让消费者离线")
	fmt.Println("   c. 发布一些QoS 1消息")
	fmt.Println("   d. 消费者重新连接后会收到离线期间的消息")

	fmt.Println("\n3. 遗嘱消息测试:")
	fmt.Println("   a. 订阅主题 'emqx/advanced/will'")
	fmt.Println("   b. 强制关闭这个客户端（不发送DISCONNECT）")
	fmt.Println("   c. 等待KeepAlive超时（约60秒）")
	fmt.Println("   d. 会收到遗嘱消息")

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("程序将继续运行，按 Ctrl+C 退出")
	fmt.Println(strings.Repeat("=", 60))

	// 保持程序运行
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !client.IsConnected() {
				fmt.Println("❌ 连接已断开，尝试重新连接...")
				if token := client.Connect(); token.Wait() && token.Error() != nil {
					fmt.Printf("重新连接失败: %v\n", token.Error())
				}
			} else {
				// 定期发布状态消息
				statusMsg := fmt.Sprintf("客户端 %s 正常运行 - 时间: %s",
					clientID, time.Now().Format("15:04:05"))
				client.Publish(statusTopic, 0, false, statusMsg)
			}
		}
	}
}

// 使用说明:
/*
1. 运行此程序:
   cd test/advanced
   go run mqtt_advanced_features.go

2. 在另一个终端测试保留消息:
   cd test/consumer
   go run mqtt_consumer.go
   （修改订阅主题为 "emqx/advanced/retained"）

3. 测试遗嘱消息:
   a. 在一个终端订阅遗嘱主题:
      mosquitto_sub -h broker.emqx.io -t "emqx/advanced/will"
   b. 强制关闭此程序（Ctrl+Z 然后 kill -9）
   c. 观察遗嘱消息

4. 测试离线消息:
   a. 启动一个持久化会话的消费者
   b. 让消费者离线
   c. 发布一些QoS 1消息
   d. 消费者重新连接后接收消息
*/
