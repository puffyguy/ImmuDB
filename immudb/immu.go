package immudb

import (
	"context"
	"fmt"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
)

type ConnectionOptions struct {
	Dir      string
	Address  string
	Port     int
	Username string
	Password string
	Database string
}

var (
	Options       *immuclient.Options
	Client        immuclient.ImmuClient
	Ctx           = context.Background()
	ConnectionSet = make(map[string]ConnectionOptions)
)

// TASK: store the connections into map[string]ConnectionOptions
func SetConnections(con []*ConnectionOptions) {
	for _, c := range con {
		if c.Address == "" || c.Port == 0 {
			fmt.Println("Please specify server address and port")
		} else if c.Username == "" || c.Password == "" || c.Database == "" {
			fmt.Println("Please specify immudb username, password and database")
		}
		if ConnectionSet[c.Address] == (ConnectionOptions{}) {
			ConnectionSet[c.Address] = ConnectionOptions{
				Dir:      c.Dir,
				Address:  c.Address,
				Port:     c.Port,
				Username: c.Username,
				Password: c.Password,
				Database: c.Database,
			}
		}
	}
}

func EstablishConnection(hostName string) string {
	if ConnectionSet[hostName] == (ConnectionOptions{}) {
		return "Please check hostname.."
	} else {
		Options = immuclient.DefaultOptions().WithAddress(ConnectionSet[hostName].Address).WithPort(ConnectionSet[hostName].Port)
		Client = immuclient.NewClient().WithOptions(Options)
		err := Client.OpenSession(Ctx, []byte(ConnectionSet[hostName].Username), []byte(ConnectionSet[hostName].Password), ConnectionSet[hostName].Database)
		if err != nil {
			fmt.Println("Error while opening connection to DB:", err)
			return ""
		}
	}
	// defer Client.CloseSession(Ctx)
	return "Connection Success.."
}

func GetActiveDB() string {
	databaseSettings, err := Client.GetDatabaseSettingsV2(Ctx)
	if err != nil {
		fmt.Println("Error while getting active database:", err)
		return ""
	}
	return databaseSettings.Database
}

func ListAllDB() []string {
	databaseList, err := Client.DatabaseListV2(Ctx)
	if err != nil {
		fmt.Println("Error while getting list of database:", err)
		return nil
	}
	dbs := []string{}
	for _, v := range databaseList.Databases {
		dbs = append(dbs, v.Name)
	}
	return dbs
}

func UseDB(dbName string) string {
	_, err := Client.UseDatabase(Ctx, &schema.Database{
		DatabaseName: dbName,
	})
	if err != nil {
		fmt.Println("Error while changing DB:", err)
		return ""
	}
	currentDB := GetActiveDB()
	if err != nil {
		fmt.Println("Error while getting active database:", err)
		return ""
	}
	return "Database changed to: " + currentDB
}

func CreateDB(dbName string, settings *schema.DatabaseNullableSettings) string {
	_, err := Client.CreateDatabaseV2(Ctx, dbName, nil)
	if err != nil {
		fmt.Println("Error while creating database:", err)
		return ""
	}
	return dbName + " database created successfully"
}

// func DeleteDB(dbName string) {
// 	deleteRes, err := Client.DeleteDatabase(Ctx, &schema.DeleteDatabaseRequest{
// 		Database: dbName,
// 	})
// 	if err != nil {
// 		fmt.Println("Error while deleting database ", err)
// 		return
// 	}
// 	fmt.Printf(deleteRes.Database, "database deleted successfully")
// }

func DBHealth() {
	health, err := Client.Health(Ctx)
	if err != nil {
		fmt.Println("Error while checking database health:", err)
		return
	}
	fmt.Println("Database health:", health)
}

func UnloadDB(dbName string) {
	unloadRes, err := Client.UnloadDatabase(Ctx, &schema.UnloadDatabaseRequest{
		Database: dbName,
	})
	if err != nil {
		fmt.Println("Error while unloadind database:", err)
		return
	}
	fmt.Println(unloadRes.Database, " database unloaded successfully")
}

func LoadDB(dbName string) {
	loadRes, err := Client.LoadDatabase(Ctx, &schema.LoadDatabaseRequest{
		Database: dbName,
	})
	if err != nil {
		fmt.Println("Error while loading database:", err)
		return
	}
	fmt.Println(loadRes.Database, "database loaded successfully")
}

func SetVal(key string, value string) *schema.TxHeader {
	setRes, err := Client.Set(Ctx, []byte(key), []byte(value))
	if err != nil {
		fmt.Println("Error while setting key value:", err)
		return nil
	}
	return setRes
}

func GetVal(key string) *schema.Entry {
	getRes, err := Client.Get(Ctx, []byte(key))
	if err != nil {
		fmt.Println("Error while getting value for provided key:", err)
		return nil
	}
	return getRes
}

func VerifiedSetVal(key string, value string) *schema.TxHeader {
	verifiedTxSet, err := Client.VerifiedSet(Ctx, []byte(key), []byte(value))
	if err != nil {
		fmt.Println("Error while setting data and verifying:", err)
		return nil
	}
	return verifiedTxSet
}

func ExpirableSet(key string, value string, expiresAt time.Time) *schema.TxHeader {
	expireSet, err := Client.ExpirableSet(Ctx, []byte(key), []byte(value), expiresAt)
	if err != nil {
		fmt.Println("Error while setting data with expiration:", err)
		return nil
	}
	return expireSet
}

func VerifiedGetVal(key string) *schema.Entry {
	verifiedTxGet, err := Client.VerifiedGet(Ctx, []byte(key))
	if err != nil {
		fmt.Println("Error while getting data with key:", err)
		return nil
	}
	return verifiedTxGet
}

func GetKeyHistory(key string) []string {
	entries, err := Client.History(Ctx, &schema.HistoryRequest{
		Key: []byte(key),
	})
	if err != nil {
		fmt.Println("Error while getting history for given key:", err)
		return nil
	}
	vals := []string{}
	for _, v := range entries.Entries {
		vals = append(vals, v.String())
	}
	return vals
}
