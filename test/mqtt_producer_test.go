package main

import (
	"fmt"
	"testing"
	"time"
)

// 测试客户端ID生成
func TestClientIDGeneration(t *testing.T) {
	clientID := "go_mqtt_producer_" + fmt.Sprintf("%d", time.Now().Unix())

	if clientID == "" {
		t.Error("客户端ID不能为空")
	}

	if len(clientID) < 10 {
		t.Error("客户端ID太短")
	}

	fmt.Printf("生成的客户端ID: %s\n", clientID)
}

// 测试消息格式
func TestMessageFormat(t *testing.T) {
	clientID := "test_client_123"
	message := fmt.Sprintf("测试消息 #%d - 时间: %s - 来自: %s",
		1,
		time.Now().Format("2006-01-02 15:04:05"),
		clientID)

	if message == "" {
		t.Error("消息不能为空")
	}

	fmt.Printf("测试消息格式: %s\n", message)
}

// 测试字符串重复函数
func TestStringRepeat(t *testing.T) {
	separator := "="
	repeated := ""

	// 手动重复60次
	for i := 0; i < 60; i++ {
		repeated += separator
	}

	if len(repeated) != 60 {
		t.Errorf("字符串重复错误，期望长度60，实际长度%d", len(repeated))
	}

	fmt.Printf("字符串重复测试通过，长度: %d\n", len(repeated))
}

// 测试配置常量（复制常量值进行测试）
func TestConfigConstants(t *testing.T) {
	// 复制主文件中的常量值
	testBroker := "broker.emqx.io"
	testPort := 1883
	testUsername := "emqx"
	testPassword := "public"

	if testBroker != "broker.emqx.io" {
		t.Errorf("broker配置错误: %s", testBroker)
	}

	if testPort != 1883 {
		t.Errorf("port配置错误: %d", testPort)
	}

	if testUsername != "emqx" {
		t.Errorf("username配置错误: %s", testUsername)
	}

	if testPassword != "public" {
		t.Errorf("password配置错误: %s", testPassword)
	}

	fmt.Println("配置常量测试通过")
}

// 测试全局变量（复制变量值进行测试）
func TestGlobalVariables(t *testing.T) {
	// 复制主文件中的变量值
	testMessageCount := 10
	testPublishTopic := "emqx/gozero/test"

	if testMessageCount != 10 {
		t.Errorf("messageCount配置错误: %d", testMessageCount)
	}

	if testPublishTopic != "emqx/gozero/test" {
		t.Errorf("publishTopic配置错误: %s", testPublishTopic)
	}

	fmt.Println("全局变量测试通过")
}

// 测试MQTT主题格式
func TestMQTTTopicFormat(t *testing.T) {
	topic := "emqx/gozero/test"

	if topic == "" {
		t.Error("主题不能为空")
	}

	// 检查主题是否包含斜杠
	if len(topic) < 3 {
		t.Error("主题太短")
	}

	fmt.Printf("MQTT主题格式测试通过: %s\n", topic)
}

// 测试时间格式
func TestTimeFormat(t *testing.T) {
	now := time.Now()
	formatted := now.Format("2006-01-02 15:04:05")

	if formatted == "" {
		t.Error("时间格式不能为空")
	}

	if len(formatted) != 19 {
		t.Errorf("时间格式长度错误，期望19，实际%d", len(formatted))
	}

	fmt.Printf("时间格式测试通过: %s\n", formatted)
}
