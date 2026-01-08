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

// 全局变量
var (
	messageCount = 10
	publishTopic = "emqx/gozero/test"
	clientID     = "go_mqtt_producer_" + fmt.Sprintf("%d", time.Now().Unix())
)

// 消息处理回调
var messageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("收到消息: %s 来自主题: %s (QoS: %d)\n",
		msg.Payload(), msg.Topic(), msg.Qos())
}

// 连接成功回调
var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("✅ 成功连接到 MQTT 服务器")
	fmt.Printf("客户端ID: %s\n", clientID)
	fmt.Printf("服务器: %s:%d\n", broker, port)
}

// 连接丢失回调
var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("❌ 连接丢失: %v\n", err)
}

// 创建MQTT客户端
func createMQTTClient() mqtt.Client {
	// 创建客户端选项
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)

	// 设置回调函数
	opts.SetDefaultPublishHandler(messageHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	// 设置连接参数
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetCleanSession(true)

	// 设置遗嘱消息（可选）
	opts.SetWill("emqx/client/status", "offline", 1, true)

	return mqtt.NewClient(opts)
}

// 连接MQTT服务器
func connectMQTT(client mqtt.Client) error {
	fmt.Println("正在连接 MQTT 服务器...")

	// 设置连接超时
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	// 异步连接
	connectChan := make(chan error, 1)
	go func() {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			connectChan <- token.Error()
		} else {
			connectChan <- nil
		}
	}()

	// 等待连接结果或超时
	select {
	case err := <-connectChan:
		return err
	case <-timeout.C:
		return fmt.Errorf("连接超时")
	}
}

// 发布测试消息
func publishTestMessages(client mqtt.Client) error {
	fmt.Printf("\n开始向主题 '%s' 发布 %d 条测试消息...\n", publishTopic, messageCount)

	for i := 1; i <= messageCount; i++ {
		// 创建消息内容
		message := fmt.Sprintf("测试消息 #%d - 时间: %s - 来自: %s",
			i,
			time.Now().Format("2006-01-02 15:04:05"),
			clientID)

		// 发布消息（QoS 0，不保留）
		token := client.Publish(publishTopic, 0, false, message)

		// 等待发布完成
		if token.Wait() && token.Error() != nil {
			return fmt.Errorf("发布消息失败: %v", token.Error())
		}

		fmt.Printf("✅ 已发布消息 %d/%d: %s\n", i, messageCount, message)

		// 间隔1秒
		if i < messageCount {
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Println("\n✅ 所有测试消息发布完成")
	return nil
}

// 订阅主题（用于验证消息）
func subscribeTestTopic(client mqtt.Client) error {
	fmt.Printf("订阅主题 '%s' 以接收消息...\n", publishTopic)

	token := client.Subscribe(publishTopic, 1, nil)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("订阅失败: %v", token.Error())
	}

	fmt.Println("✅ 主题订阅成功")
	return nil
}

// 优雅退出处理
func setupGracefulShutdown(client mqtt.Client) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n\n收到信号: %v，正在优雅退出...\n", sig)

		// 发布离线状态
		client.Publish("emqx/client/status", 1, true, "offline")
		time.Sleep(100 * time.Millisecond)

		// 断开连接
		client.Disconnect(250)
		fmt.Println("✅ MQTT连接已断开")

		os.Exit(0)
	}()
}

// 显示使用信息
func showUsageInfo() {
	separator := strings.Repeat("=", 60)
	fmt.Println(separator)
	fmt.Println("MQTT 测试生产者")
	fmt.Println(separator)
	fmt.Printf("服务器: %s:%d\n", broker, port)
	fmt.Printf("主题: %s\n", publishTopic)
	fmt.Printf("消息数量: %d\n", messageCount)
	fmt.Printf("客户端ID: %s\n", clientID)
	fmt.Println(separator)
	fmt.Println("按 Ctrl+C 退出程序")
	fmt.Println(separator)
	fmt.Println()
}

func main() {
	// 显示使用信息
	showUsageInfo()

	// 创建MQTT客户端
	client := createMQTTClient()

	// 设置优雅退出
	setupGracefulShutdown(client)

	// 连接MQTT服务器
	if err := connectMQTT(client); err != nil {
		log.Fatalf("❌ 连接MQTT服务器失败: %v", err)
	}

	// 订阅主题（可选，用于接收自己发布的消息）
	if err := subscribeTestTopic(client); err != nil {
		fmt.Printf("⚠️  订阅失败（不影响发布）: %v\n", err)
	}

	// 发布测试消息
	if err := publishTestMessages(client); err != nil {
		log.Fatalf("❌ 发布消息失败: %v", err)
	}

	// 保持程序运行，等待用户中断
	separator := strings.Repeat("=", 60)
	fmt.Println("\n" + separator)
	fmt.Println("测试完成！程序将继续运行以保持连接")
	fmt.Println("按 Ctrl+C 退出程序")
	fmt.Println(separator)

	// 保持连接，定期发送心跳消息
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !client.IsConnected() {
				fmt.Println("❌ 连接已断开，尝试重新连接...")
				if err := connectMQTT(client); err != nil {
					fmt.Printf("重新连接失败: %v\n", err)
				}
			} else {
				// 发送心跳消息
				heartbeat := fmt.Sprintf("心跳 - %s", time.Now().Format("15:04:05"))
				client.Publish("emqx/client/heartbeat", 0, false, heartbeat)
			}
		}
	}
}
