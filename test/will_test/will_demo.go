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
	willTopic    = "emqx/will/test"    // 遗嘱消息主题
	statusTopic  = "emqx/will/status"  // 状态主题
	controlTopic = "emqx/will/control" // 控制主题
)

// 创建带有遗嘱消息的客户端
func createClientWithWill(clientID string, willMessage string, keepAlive time.Duration) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)

	// 设置遗嘱消息（Last Will and Testament）
	// 参数：主题，消息内容，QoS，是否保留
	opts.SetWill(willTopic, willMessage, 1, false)

	// 设置较短的KeepAlive以便快速检测断开
	opts.SetKeepAlive(keepAlive)
	opts.SetPingTimeout(5 * time.Second)

	// 设置连接回调
	opts.OnConnect = func(client mqtt.Client) {
		fmt.Printf("✅ 客户端 '%s' 已连接\n", clientID)

		// 发布连接状态
		statusMsg := fmt.Sprintf("客户端 '%s' 已连接 - 时间: %s",
			clientID, time.Now().Format("2006-01-02 15:04:05"))
		client.Publish(statusTopic, 0, false, statusMsg)
	}

	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("❌ 客户端 '%s' 连接丢失: %v\n", clientID, err)
	}

	return mqtt.NewClient(opts)
}

// 创建监控客户端（用于接收遗嘱消息）
func createMonitorClient() mqtt.Client {
	monitorID := "will_monitor_" + fmt.Sprintf("%d", time.Now().Unix())

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID(monitorID)
	opts.SetUsername(username)
	opts.SetPassword(password)

	// 设置消息处理回调
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("\n📨 监控收到消息:\n")
		fmt.Printf("   主题: %s\n", msg.Topic())
		fmt.Printf("   内容: %s\n", msg.Payload())
		fmt.Printf("   时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))

		if msg.Topic() == willTopic {
			fmt.Println("   ⚰️  这是一条遗嘱消息！")
			fmt.Println("   💡 说明有客户端异常断开连接")
		}
	})

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("监控客户端连接失败: %v", token.Error())
	}

	// 订阅遗嘱主题和状态主题
	token := client.Subscribe(willTopic, 1, nil)
	token.Wait()
	if token.Error() != nil {
		log.Fatalf("订阅遗嘱主题失败: %v", token.Error())
	}

	token2 := client.Subscribe(statusTopic, 0, nil)
	token2.Wait()
	if token2.Error() != nil {
		log.Fatalf("订阅状态主题失败: %v", token2.Error())
	}

	token3 := client.Subscribe(controlTopic, 0, nil)
	token3.Wait()

	fmt.Println("✅ 监控客户端已启动并订阅主题")
	return client
}

// 测试用例1：正常断开（不应触发遗嘱消息）
func testNormalDisconnect() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("测试用例 1: 正常断开连接")
	fmt.Println(strings.Repeat("=", 60))

	clientID := "test_client_normal_" + fmt.Sprintf("%d", time.Now().Unix())
	willMessage := fmt.Sprintf("客户端 '%s' 异常断开", clientID)

	client := createClientWithWill(clientID, willMessage, 30*time.Second)

	// 连接
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("连接失败: %v", token.Error())
		return
	}

	// 等待连接稳定
	time.Sleep(2 * time.Second)

	// 发布状态消息
	statusMsg := fmt.Sprintf("客户端 '%s' 正常运行", clientID)
	client.Publish(statusTopic, 0, false, statusMsg)

	fmt.Println("等待5秒...")
	time.Sleep(5 * time.Second)

	// 正常断开连接
	fmt.Println("发送正常DISCONNECT包...")
	client.Disconnect(250)

	fmt.Println("✅ 正常断开完成")
	fmt.Println("预期结果: 不会触发遗嘱消息")
	fmt.Println("原因: 正常发送了DISCONNECT包")
}

// 测试用例2：异常断开（应触发遗嘱消息）
func testAbnormalDisconnect(monitor mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("测试用例 2: 异常断开连接")
	fmt.Println(strings.Repeat("=", 60))

	clientID := "test_client_abnormal_" + fmt.Sprintf("%d", time.Now().Unix())
	willMessage := fmt.Sprintf("客户端 '%s' 异常断开 - 时间: %s",
		clientID, time.Now().Format("2006-01-02 15:04:05"))

	client := createClientWithWill(clientID, willMessage, 10*time.Second) // 较短的KeepAlive

	// 连接
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("连接失败: %v", token.Error())
		return
	}

	// 发布状态消息
	statusMsg := fmt.Sprintf("客户端 '%s' 即将异常断开", clientID)
	client.Publish(statusTopic, 0, false, statusMsg)

	fmt.Println("客户端已连接，KeepAlive=10秒")
	fmt.Println("模拟异常断开（不发送DISCONNECT包）...")

	// 模拟网络断开（在实际测试中，可以关闭网络或强制结束进程）
	// 这里我们只是记录，实际测试需要强制断开
	fmt.Println("\n⚠️  在实际测试中，需要:")
	fmt.Println("   1. 强制关闭网络连接")
	fmt.Println("   2. 或强制结束进程（kill -9）")
	fmt.Println("   3. 或等待KeepAlive超时（约10秒）")

	fmt.Println("\n监控客户端应该在大约10秒后收到遗嘱消息")

	// 发送控制命令
	controlMsg := fmt.Sprintf("开始异常断开测试 - 客户端: %s", clientID)
	monitor.Publish(controlTopic, 0, false, controlMsg)

	// 保持连接，让用户手动测试
	fmt.Println("\n按Enter键继续（在实际测试中不要按，让客户端超时）...")
	fmt.Scanln()

	// 正常断开（避免实际触发遗嘱消息）
	client.Disconnect(250)
}

// 测试用例3：多个客户端的遗嘱消息
func testMultipleClients(monitor mqtt.Client) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("测试用例 3: 多个客户端的遗嘱消息")
	fmt.Println(strings.Repeat("=", 60))

	// 创建3个带有遗嘱消息的客户端
	clients := make([]mqtt.Client, 3)
	clientIDs := make([]string, 3)

	for i := 0; i < 3; i++ {
		clientID := fmt.Sprintf("multi_client_%d_%d", i, time.Now().Unix())
		clientIDs[i] = clientID

		willMessage := fmt.Sprintf("客户端 '%s' 异常断开 - 序号: %d", clientID, i)

		client := createClientWithWill(clientID, willMessage, 15*time.Second)
		clients[i] = client

		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("客户端 %d 连接失败: %v", i, token.Error())
			continue
		}

		statusMsg := fmt.Sprintf("多客户端测试 - 客户端 %d 已连接", i)
		client.Publish(statusTopic, 0, false, statusMsg)

		fmt.Printf("✅ 客户端 %d ('%s') 已连接\n", i, clientID)
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\n所有客户端已连接，KeepAlive=15秒")
	fmt.Println("可以模拟其中一些客户端异常断开")

	// 发送控制命令
	controlMsg := "多客户端遗嘱测试已就绪"
	monitor.Publish(controlTopic, 0, false, controlMsg)

	fmt.Println("\n按Enter键清理所有客户端...")
	fmt.Scanln()

	// 正常断开所有客户端
	for i, client := range clients {
		if client != nil && client.IsConnected() {
			client.Disconnect(250)
			fmt.Printf("客户端 %d 已正常断开\n", i)
		}
	}
}

// 显示使用信息
func showUsageInfo() {
	separator := strings.Repeat("=", 60)
	fmt.Println(separator)
	fmt.Println("MQTT 遗嘱消息（Last Will）测试程序")
	fmt.Println(separator)
	fmt.Printf("服务器: %s:%d\n", broker, port)
	fmt.Printf("遗嘱主题: %s\n", willTopic)
	fmt.Printf("状态主题: %s\n", statusTopic)
	fmt.Println(separator)
	fmt.Println("什么是遗嘱消息（Last Will and Testament）?")
	fmt.Println("   1. 客户端在连接时指定一条遗嘱消息")
	fmt.Println("   2. 如果客户端异常断开（未发送DISCONNECT）")
	fmt.Println("   3. 服务器会自动发布这条遗嘱消息")
	fmt.Println("   4. 其他订阅者会收到客户端异常断开通知")
	fmt.Println(separator)
	fmt.Println("测试用例:")
	fmt.Println("   1. 正常断开 - 不应触发遗嘱消息")
	fmt.Println("   2. 异常断开 - 应触发遗嘱消息")
	fmt.Println("   3. 多客户端测试")
	fmt.Println(separator)
}

// 优雅退出处理
func setupGracefulShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n\n收到信号: %v，正在退出...\n", sig)
		os.Exit(0)
	}()
}

func main() {
	showUsageInfo()

	setupGracefulShutdown()

	// 创建监控客户端
	fmt.Println("\n启动监控客户端...")
	monitor := createMonitorClient()
	defer monitor.Disconnect(250)

	// 等待监控客户端稳定
	time.Sleep(2 * time.Second)

	// 运行测试用例
	testNormalDisconnect()

	time.Sleep(3 * time.Second)

	testAbnormalDisconnect(monitor)

	time.Sleep(3 * time.Second)

	testMultipleClients(monitor)

	// 显示实际测试说明
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🧪 实际测试说明")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\n要进行实际的遗嘱消息测试，请执行以下步骤:")

	fmt.Println("\n1. 在一个终端运行此程序作为监控:")
	fmt.Println("   cd test/advanced")
	fmt.Println("   go run will_test.go")

	fmt.Println("\n2. 在另一个终端创建测试客户端:")
	fmt.Println("   使用以下命令创建带有遗嘱消息的客户端:")
	fmt.Println("   mosquitto_sub -h broker.emqx.io \\")
	fmt.Println("     -i 'test_client' -u emqx -P public \\")
	fmt.Println("     -t 'emqx/will/status' \\")
	fmt.Println("     --will-topic 'emqx/will/test' \\")
	fmt.Println("     --will-payload '客户端异常断开' \\")
	fmt.Println("     --will-qos 1 --will-retain false")

	fmt.Println("\n3. 异常断开测试:")
	fmt.Println("   a. 强制结束mosquitto_sub进程（Ctrl+\\ 或 kill -9）")
	fmt.Println("   b. 或断开网络连接")
	fmt.Println("   c. 等待KeepAlive超时（默认60秒）")
	fmt.Println("   d. 观察监控终端是否收到遗嘱消息")

	fmt.Println("\n4. 正常断开测试:")
	fmt.Println("   a. 正常退出mosquitto_sub（Ctrl+C）")
	fmt.Println("   b. 观察监控终端是否收到遗嘱消息（应该不会）")

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("程序运行完成")
	fmt.Println(strings.Repeat("=", 60))

	// 保持监控运行
	fmt.Println("\n监控客户端继续运行，按 Ctrl+C 退出...")
	select {}
}

// 使用说明:
/*
遗嘱消息（Last Will and Testament）是MQTT的一个重要特性，
用于检测客户端是否异常断开。常见应用场景：

1. 设备监控：物联网设备异常离线时发送警报
2. 会话管理：用户异常断开时清理会话状态
3. 系统监控：服务异常停止时通知其他服务

关键点：
- 只有异常断开（未发送DISCONNECT）才会触发遗嘱消息
- 正常断开不会触发遗嘱消息
- 遗嘱消息的QoS和保留标志可以单独设置
- KeepAlive时间影响检测速度
*/
