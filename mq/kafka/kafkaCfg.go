package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/xdg-go/scram"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

func NewUUIDStr() string {
	return strings.ReplaceAll(uuid.NewV4().String(), "-", "")
}

const (
	// kafka版本说明：https://zhuanlan.zhihu.com/p/359573455
	DEFAULT_VERSION = "2.4.0"
)

type Config struct {
	Version string
	Brokers string

	// consumer
	Topics string
	// 1个partition只能被同group的一个consumer消费，同组的consumer则起到均衡效果
	Group    string
	Assignor string
	Oldest   bool

	// Sasl
	SaslEnable bool

	// UserName is the authentication identity (authcid) to present for SASL/PLAIN or SASL/SCRAM authentication
	UserName string
	// Password for SASL/PLAIN authentication
	PassWord  string
	Algorithm string
	CertFile  string
	CaFile    string
	KeyFile   string
	VerifySSL bool

	// kerberos
	KerberosConfig     *sarama.GSSAPIConfig
	Realm              string
	ServiceName        string
	KeyTabPath         string
	KerberosConfigPath string
	// TLS
	UseTLS bool

	// OAUTHBEARER鉴权模式需要的参数
	TokenProvider sarama.AccessTokenProvider

	//增加backoff
	Backoff string
	saram   *sarama.Config

	MaxProcessingTimeMilliSecond int
}

func (c *Config) newTLSConfiguration() (*tls.Config, error) {
	t := &tls.Config{
		InsecureSkipVerify: c.VerifySSL,
	}
	if c.CertFile != "" && c.KeyFile != "" && c.CaFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}

		caCert, err := ioutil.ReadFile(c.CaFile)
		if err != nil {
			return nil, err
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: c.VerifySSL,
		}
	}
	return t, nil
}

func (c *Config) InitSarama() error {
	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	saramaConfig := sarama.NewConfig()
	// A user-provided string sent with every request to the brokers for logging,
	// debugging, and auditing purposes. Defaults to "sarama", but you should
	// probably set it to something specific to your application.
	saramaConfig.ClientID = "kafka_client"
	// Whether to maintain a full set of metadata for all topics, or just
	// the minimal set that has been necessary so far. The full set is simpler
	// and usually more convenient, but can take up a substantial amount of
	// memory if you have many topics and partitions. Defaults to true.

	saramaConfig.Metadata.Full = false // Defaults to true.

	if c.Group != "" {
		// 调整会话超时时间和心跳间隔
		//saramaConfig.Consumer.Group.Session.Timeout = 30 * time.Second   //消费者组会话超时
		//saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second //消费者发送心跳的时间间隔
		//saramaConfig.Consumer.MaxProcessingTime = 10 * time.Minute       //如果处理时间超过此值，可能会导致消息积压或订阅被放弃。
		// 自动提交偏移量
		//saramaConfig.Consumer.Offsets.AutoCommit.Enable = false //不自动提交偏移量
		//saramaConfig.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
		// 调整拉取参数
		//saramaConfig.Consumer.Fetch.Default = 1024      // 默认拉取 1 KB 数据
		//saramaConfig.Consumer.Fetch.Min = 1024          // 最小拉取 1 字节数据
		//saramaConfig.Net.ReadTimeout = 60 * time.Second // 网络读取超时时间
	}

	if c.MaxProcessingTimeMilliSecond > 100 {
		saramaConfig.Consumer.MaxProcessingTime = time.Duration(c.MaxProcessingTimeMilliSecond) * time.Millisecond
	}

	ver, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return errors.Errorf("Error parsing Kafka version: %v", err)
	}
	saramaConfig.Version = ver
	c.Algorithm = strings.ToLower(c.Algorithm)
	if c.SaslEnable {
		if c.KerberosConfig == nil {
			// SASL based authentication with broker. While there are multiple SASL authentication methods
			// the current implementation is limited to plaintext (SASL/PLAIN) authentication
			saramaConfig.Net.SASL.Enable = true
			saramaConfig.Net.SASL.User = c.UserName
			saramaConfig.Net.SASL.Password = c.PassWord
			saramaConfig.Net.SASL.Handshake = true
			if c.Algorithm == "plain" || c.Algorithm == sarama.SASLTypePlaintext {
				saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			} else if c.Algorithm == "oauthbearer" || c.Algorithm == sarama.SASLTypeOAuth {
				saramaConfig.Net.SASL.TokenProvider = c.TokenProvider
				saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
			} else if c.Algorithm == "sha512" || c.Algorithm == sarama.SASLTypeSCRAMSHA512 {
				saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
				saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			} else if c.Algorithm == "sha256" || c.Algorithm == sarama.SASLTypeSCRAMSHA256 {
				saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
				saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			} else if c.Algorithm == "gssapi" || c.Algorithm == sarama.SASLTypeGSSAPI {
				saramaConfig.Net.SASL.Enable = true
				saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
				saramaConfig.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
					AuthType:           sarama.KRB5_KEYTAB_AUTH,
					Realm:              c.Realm,
					ServiceName:        c.ServiceName,
					Username:           c.UserName,
					KeyTabPath:         c.KeyTabPath,
					KerberosConfigPath: c.KerberosConfigPath,
				}
			} else {
				return errors.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", c.Algorithm)
			}
		} else {
			// 如果有kerberosconfig配置
			saramaConfig.Net.SASL.Enable = true
			saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
			saramaConfig.Net.SASL.GSSAPI = *c.KerberosConfig
		}
	}

	if c.UseTLS {
		saramaConfig.Net.TLS.Enable = true
		tlsConf, err := c.newTLSConfiguration()
		if err != nil {
			return err
		}
		saramaConfig.Net.TLS.Config = tlsConf
	}

	// consumer
	if c.Assignor != "" {
		// Strategy for allocating topic partitions to members (default BalanceStrategyRange)
		switch c.Assignor {
		//case "sticky":
		//	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
		//case "roundrobin":
		//	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
		//case "range":
		//	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
		case "sticky":
			saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		case "roundrobin":
			saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
		case "range":
			saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
		default:
			return errors.Errorf("Unrecognized consumer group partition assignor: %s, acceptable assignors are sticky/roundrobin/range", c.Assignor)
		}
	}

	if c.Oldest {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	// producer
	// The total number of times to retry sending a message (default 3).
	saramaConfig.Producer.Retry.Max = 3
	// The level of acknowledgement reliability needed from the broker (defaults
	// to WaitForLocal).
	saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	// If enabled, successfully delivered messages will be returned on the
	// Successes channel.Must be true to be used in a SyncProducer
	saramaConfig.Producer.Return.Successes = true

	//增加backoff 的设置
	if c.Backoff != "" {
		intbackoff, _ := strconv.Atoi(c.Backoff)
		saramaConfig.Producer.Retry.Backoff = time.Duration(intbackoff) * time.Millisecond
		//comsumer
		saramaConfig.Consumer.Retry.Backoff = time.Duration(intbackoff) * time.Millisecond
	}

	c.saram = saramaConfig

	return nil
}

var SHA256 scram.HashGeneratorFcn = sha256.New
var SHA512 scram.HashGeneratorFcn = sha512.New

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
