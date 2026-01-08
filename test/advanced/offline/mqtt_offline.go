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
	offlineTestTopic = "emqx/offline/test"
	controlTopic     = "emqx/offline/control"
)

// 全局变量
var (
	clientID      = "offline_test_" + fmt.Sprintf("%d", time.Now().Unix())
	receivedCount = 0
	isOnline      = true
)

// 消息处理回调
var messageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	receivedCount++

	fmt.Printf("\n📨 收到离线消息 #%d\n", receivedCount)
	fmt.Printf("   主题: %s\n", msg.Topic())
	fmt.Printf("   内容: %s\n", msg.Payload())
	fmt.Printf("   QoS: %d\n", msg.Qos())
	fmt.Printf("   时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	// 检查是否是控制消息
	if msg.Topic() == controlTopic {
		cmd := string(msg.Payload())
		fmt.Printf("   控制命令: %s\n", cmd)

		if cmd == "go_offline" {
			fmt.Println("   ⚡ 收到离线命令，模拟客户端离线...")
			go simulateOffline(client)
		} else if cmd == "go_online" {
			fmt.Println("   ⚡ 收到上线命令，重新连接...")
			go simulateReconnect(client)
		}
	}

	msg.Ack()
}

// 创建持久化会话客户端（CleanSession=false）
func createPersistentClient() mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID) // 重要：相同的ClientID用于恢复会话
	opts.SetUsername(username)
	opts.SetPassword(password)

	opts.SetDefaultPublishHandler(messageHandler)

	// 关键设置：CleanSession=false 启用持久化会话
	opts.SetCleanSession(false)

	// 设置自动重连
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)

	// 设置消息存储（内存存储）
	opts.SetStore(mqtt.NewMemoryStore())

	// 设置KeepAlive较短以便快速检测离线
	opts.SetKeepAlive(20 * time.Second)
	opts.SetPingTimeout(5 * time.Second)

	// 设置连接回调
	opts.OnConnect = func(client mqtt.Client) {
		fmt.Println("✅ 连接到MQTT服务器")
		isOnline = true

		// 重新订阅主题
		token := client.Subscribe(offlineTestTopic, 1, nil)
		token.Wait()
		if token.Error() == nil {
			fmt.Println("✅ 重新订阅主题成功")
		}

		token2 := client.Subscribe(controlTopic, 1, nil)
		token2.Wait()
	}

	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("❌ 连接丢失: %v\n", err)
		isOnline = false
	}

	return mqtt.NewClient(opts)
}

// 模拟客户端离线
func simulateOffline(client mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🔌 模拟客户端离线")
	fmt.Println(strings.Repeat("=", 60))

	// 发布离线通知
	offlineMsg := fmt.Sprintf("客户端 %s 即将离线 - 时间: %s",
		clientID, time.Now().Format("2006-01-02 15:04:05"))
	client.Publish(controlTopic, 1, false, offlineMsg)
	time.Sleep(1 * time.Second)

	// 断开连接（但不清理会话）
	fmt.Println("断开连接（保持会话）...")
	client.Disconnect(250)
	isOnline = false

	fmt.Println("\n客户端已离线，但会话仍保留在服务器")
	fmt.Println("在此期间发布到 offlineTestTopic 的QoS 1/2消息将被保留")
	fmt.Println("等待60秒后重新连接...")

	time.Sleep(60 * time.Second)

	// 自动重新连接
	simulateReconnect(client)
}

// 模拟重新连接
func simulateReconnect(client mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🔌 模拟客户端重新连接")
	fmt.Println(strings.Repeat("=", 60))

	fmt.Println("正在重新连接MQTT服务器...")

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Printf("重新连接失败: %v\n", token.Error())
		return
	}

	fmt.Println("✅ 重新连接成功")
	fmt.Println("应该收到离线期间发布的消息（如果使用QoS 1/2）")

	// 发布重新连接通知
	onlineMsg := fmt.Sprintf("客户端 %s 已重新连接 - 时间: %s",
		clientID, time.Now().Format("2006-01-02 15:04:05"))
	client.Publish(controlTopic, 1, false, onlineMsg)
}

// 创建生产者客户端（用于发布测试消息）
func createProducerClient() mqtt.Client {
	producerID := "producer_" + fmt.Sprintf("%d", time.Now().Unix())

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(producerID)
	opts.SetUsername(username)
	opts.SetPassword(password)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("生产者连接失败: %v", token.Error())
	}

	return client
}

// 发布离线测试消息
func publishOfflineTestMessages(producer mqtt.Client, count int) {
	fmt.Printf("\n发布 %d 条离线测试消息...\n", count)

	for i := 1; i <= count; i++ {
		message := fmt.Sprintf("离线测试消息 #%d - 发布时间: %s",
			i, time.Now().Format("2006-01-02 15:04:05"))

		// 使用QoS 1确保消息送达
		token := producer.Publish(offlineTestTopic, 1, false, message)
		token.Wait()

		if token.Error() != nil {
			fmt.Printf("发布消息 %d 失败: %v\n", i, token.Error())
		} else {
			fmt.Printf("✅ 已发布消息 #%d (QoS 1)\n", i)
		}

		time.Sleep(2 * time.Second)
	}
}

// 优雅退出处理
func setupGracefulShutdown(client mqtt.Client) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n\n收到信号: %v，正在优雅退出...\n", sig)

		// 清理会话（正常退出）
		client.Disconnect(250)
		fmt.Println("✅ MQTT连接已断开")

		os.Exit(0)
	}()
}

// 显示使用信息
func showUsageInfo() {
	separator := strings.Repeat("=", 60)
	fmt.Println(separator)
	fmt.Println("MQTT 离线消息测试程序")
	fmt.Println(separator)
	fmt.Printf("服务器: %s:%d\n", broker, port)
	fmt.Printf("客户端ID: %s\n", clientID)
	fmt.Printf("测试主题: %s\n", offlineTestTopic)
	fmt.Printf("控制主题: %s\n", controlTopic)
	fmt.Println(separator)
	fmt.Println("关键设置: CleanSession=false")
	fmt.Println("这意味着:")
	fmt.Println("   1. 服务器会保留客户端的订阅信息")
	fmt.Println("   2. 离线期间的QoS 1/2消息会被保留")
	fmt.Println("   3. 重新连接后会收到离线期间的消息")
	fmt.Println(separator)
	fmt.Println("测试步骤:")
	fmt.Println("   1. 启动此消费者程序")
	fmt.Println("   2. 在另一个终端运行生产者发布消息")
	fmt.Println("   3. 发送 'go_offline' 控制命令")
	fmt.Println("   4. 在消费者离线时发布更多消息")
	fmt.Println("   5. 等待消费者重新连接")
	fmt.Println("   6. 观察是否收到离线期间的消息")
	fmt.Println(separator)
	fmt.Println("按 Ctrl+C 退出程序")
	fmt.Println(separator)
	fmt.Println()
}

func main() {
	showUsageInfo()

	// 创建持久化会话客户端
	client := createPersistentClient()

	setupGracefulShutdown(client)

	// 连接MQTT服务器
	fmt.Println("正在连接MQTT服务器（持久化会话）...")
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ 连接MQTT服务器失败: %v", token.Error())
	}

	// 订阅测试主题
	fmt.Println("订阅测试主题...")
	token := client.Subscribe(offlineTestTopic, 1, nil)
	token.Wait()
	if token.Error() != nil {
		log.Fatalf("订阅测试主题失败: %v", token.Error())
	}

	token2 := client.Subscribe(controlTopic, 1, nil)
	token2.Wait()
	if token2.Error() != nil {
		log.Fatalf("订阅控制主题失败: %v", token2.Error())
	}

	fmt.Println("✅ 所有主题订阅成功")

	// 显示测试说明
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📋 测试说明")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\n要测试离线消息功能，请执行以下操作:")
	fmt.Println("\n1. 在另一个终端启动生产者:")
	fmt.Println("   cd test")
	fmt.Println("   go run mqtt_producer.go")
	fmt.Println("   （需要修改主题为 'emqx/offline/test'）")

	fmt.Println("\n2. 或者使用mosquitto_pub发布消息:")
	fmt.Println("   mosquitto_pub -h broker.emqx.io -t 'emqx/offline/test' \\")
	fmt.Println("     -m '测试消息' -q 1 -u emqx -P public")

	fmt.Println("\n3. 发送控制命令让此客户端离线:")
	fmt.Println("   mosquitto_pub -h broker.emqx.io -t 'emqx/offline/control' \\")
	fmt.Println("     -m 'go_offline' -u emqx -P public")

	fmt.Println("\n4. 在客户端离线时发布更多消息")

	fmt.Println("\n5. 等待60秒或发送上线命令:")
	fmt.Println("   mosquitto_pub -h broker.emqx.io -t 'emqx/offline/control' \\")
	fmt.Println("     -m 'go_online' -u emqx -P public")

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("程序运行中，等待消息...")
	fmt.Println(strings.Repeat("=", 60))

	// 保持程序运行
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastCount := 0

	for {
		select {
		case <-ticker.C:
			if isOnline {
				// 发布心跳
				heartbeat := fmt.Sprintf("消费者在线 - 已收消息: %d - 时间: %s",
					receivedCount, time.Now().Format("15:04:05"))
				client.Publish(controlTopic, 0, false, heartbeat)

				// 显示消息接收统计
				if receivedCount > lastCount {
					fmt.Printf("\n📊 消息统计: 总共收到 %d 条消息\n", receivedCount)
					lastCount = receivedCount
				}
			} else {
				fmt.Println("⏸️  客户端当前离线...")
			}
		}
	}
}

// 生产者测试程序（可选单独运行）
/*
package main

import (
	"fmt"
	"time"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://broker.emqx.io:1883")
	opts.SetClientID("offline_producer")
	opts.SetUsername("emqx")
	opts.SetPassword("public")

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// 发布测试消息
	for i := 1; i <= 5; i++ {
		msg := fmt.Sprintf("测试消息 #%d - %s", i, time.Now().Format("15:04:05"))
		token := client.Publish("emqx/offline/test", 1, false, msg)
		token.Wait()
		fmt.Printf("已发布: %s\n", msg)
		time.Sleep(3 * time.Second)
	}

	client.Disconnect(250)
}
*/
