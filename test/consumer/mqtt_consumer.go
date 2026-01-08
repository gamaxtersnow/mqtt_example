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

// 消费者全局变量
var (
	subscribeTopic = "emqx/advanced/retained"
	consumerID     = "go_mqtt_consumer_" + fmt.Sprintf("%d", time.Now().Unix())
	receivedCount  = 0
	startTime      time.Time
)

// 消息处理回调
var consumerMessageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	receivedCount++

	fmt.Printf("\n📨 收到消息 #%d\n", receivedCount)
	fmt.Printf("   主题: %s\n", msg.Topic())
	fmt.Printf("   内容: %s\n", msg.Payload())
	fmt.Printf("   QoS: %d\n", msg.Qos())
	fmt.Printf("   时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	// 确认消息已处理
	msg.Ack()
}

// 连接成功回调
var consumerConnectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("✅ 成功连接到 MQTT 服务器")
	fmt.Printf("   客户端ID: %s\n", consumerID)
	fmt.Printf("   服务器: %s:%d\n", broker, port)
	fmt.Printf("   订阅主题: %s\n", subscribeTopic)
}

// 连接丢失回调
var consumerConnectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("❌ 连接丢失: %v\n", err)
}

// 创建MQTT消费者客户端
func createMQTTConsumer() mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(consumerID)
	opts.SetUsername(username)
	opts.SetPassword(password)

	opts.SetDefaultPublishHandler(consumerMessageHandler)
	opts.OnConnect = consumerConnectHandler
	opts.OnConnectionLost = consumerConnectLostHandler

	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetCleanSession(false)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(10 * time.Second)

	opts.SetWill("emqx/consumer/status", "offline", 1, true)

	return mqtt.NewClient(opts)
}

// 连接MQTT服务器
func connectConsumer(client mqtt.Client) error {
	fmt.Println("正在连接 MQTT 服务器...")

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	return nil
}

// 订阅主题
func subscribeConsumerTopic(client mqtt.Client) error {
	fmt.Printf("正在订阅主题 '%s'...\n", subscribeTopic)

	token := client.Subscribe(subscribeTopic, 1, nil)
	token.Wait()

	if token.Error() != nil {
		return token.Error()
	}

	fmt.Println("✅ 主题订阅成功")
	return nil
}

// 优雅退出处理
func setupConsumerGracefulShutdown(client mqtt.Client) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n\n收到信号: %v，正在优雅退出...\n", sig)

		client.Publish("emqx/consumer/status", 1, true, "offline")
		time.Sleep(100 * time.Millisecond)

		fmt.Printf("\n📊 消费统计:\n")
		fmt.Printf("   总共收到消息: %d 条\n", receivedCount)
		fmt.Printf("   运行时间: %s\n", time.Since(startTime).Round(time.Second))

		client.Disconnect(250)
		fmt.Println("✅ MQTT连接已断开")

		os.Exit(0)
	}()
}

// 显示消费者使用信息
func showConsumerUsageInfo() {
	separator := strings.Repeat("=", 60)
	fmt.Println(separator)
	fmt.Println("MQTT 测试消费者")
	fmt.Println(separator)
	fmt.Printf("服务器: %s:%d\n", broker, port)
	fmt.Printf("订阅主题: %s\n", subscribeTopic)
	fmt.Printf("客户端ID: %s\n", consumerID)
	fmt.Printf("QoS: 1 (至少送达一次)\n")
	fmt.Println(separator)
	fmt.Println("按 Ctrl+C 退出程序")
	fmt.Println(separator)
	fmt.Println()
}

func main() {
	startTime = time.Now()

	showConsumerUsageInfo()

	client := createMQTTConsumer()

	setupConsumerGracefulShutdown(client)

	if err := connectConsumer(client); err != nil {
		log.Fatalf("❌ 连接MQTT服务器失败: %v", err)
	}

	if err := subscribeConsumerTopic(client); err != nil {
		log.Fatalf("❌ 订阅主题失败: %v", err)
	}

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("✅ 消费者已就绪，正在等待消息...")
	fmt.Println("   运行生产者程序或通过其他客户端向主题发布消息")
	fmt.Println("   按 Ctrl+C 退出程序")
	fmt.Println(strings.Repeat("=", 60))

	// 保持程序运行
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	lastCount := 0
	lastCheck := time.Now()

	for {
		select {
		case <-ticker.C:
			if !client.IsConnected() {
				fmt.Println("❌ 连接已断开，尝试重新连接...")
				if err := connectConsumer(client); err != nil {
					fmt.Printf("重新连接失败: %v\n", err)
				} else {
					subscribeConsumerTopic(client)
				}
			} else {
				heartbeat := fmt.Sprintf("消费者心跳 - %s - 已收消息: %d",
					time.Now().Format("15:04:05"), receivedCount)
				client.Publish("emqx/consumer/heartbeat", 0, false, heartbeat)

				now := time.Now()
				timeDiff := now.Sub(lastCheck).Seconds()
				if timeDiff >= 60 && receivedCount > lastCount {
					rate := float64(receivedCount-lastCount) / timeDiff
					fmt.Printf("\n📈 消息接收速率: %.2f 条/秒\n", rate)
					lastCount = receivedCount
					lastCheck = now
				}
			}
		}
	}
}
