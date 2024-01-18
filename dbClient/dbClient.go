package dbClient

import (
	"database/sql"
	"fmt"
	logging "github.com/jifuy/commongo/loging"
	"strconv"
	"strings"
	"time"
)

var DbClients = make(map[string]*sql.DB, 0)

// 客户端对象
type DbInfo struct {
	DbName      string //连接名
	DbType      string
	Host        string
	Port        string
	UserName    string
	PassWord    string
	DataBase    string //连接的库名
	MaxOpenConn int
	MaxIdleConn int
	Dsn         string
}

func SetUpDb(m DbInfo) (*sql.DB, error) {
	var openUrl string
	switch m.DbType {
	case "mysql":
		openUrl = fmt.Sprintf("%s:%s@%s(%s:%s)/%s", m.UserName, m.PassWord, "tcp", m.Host, m.Port, m.DataBase) + "?charset=utf8mb4&parseTime=true&loc=Local"
	case "dm":
		openUrl = fmt.Sprintf("%s://%s:%s@%s:%s", "dm", m.UserName, m.PassWord, m.Host, m.Port)
	}
	if m.Dsn != "" {
		openUrl = m.Dsn
	}
	sDb, err := sql.Open(m.DbType, openUrl)
	if err != nil {
		return nil, err
	}
	sDb.SetConnMaxLifetime(time.Second * 20)
	if m.MaxOpenConn < 1 {
		m.MaxOpenConn = 10
	}
	if m.MaxIdleConn < 1 {
		m.MaxIdleConn = 5
	}

	if err = sDb.Ping(); err != nil {
		return nil, err
	}
	DbClients[m.DbName] = sDb
	return sDb, nil
}

func UpdateSql(SqlDb *sql.DB, tableName string, upFields map[string]interface{}, termFields map[string]interface{}) (int64, error) {
	updateFields := make([]string, 0)
	updateValues := make([]interface{}, 0)
	for key, value := range upFields {
		updateFields = append(updateFields, fmt.Sprintf("%s = ?", key))
		updateValues = append(updateValues, value)
	}
	whereFields := make([]string, 0)
	whereValues := make([]interface{}, 0)
	for key, value := range termFields {
		whereFields = append(whereFields, fmt.Sprintf("%s = ?", key))
		whereValues = append(whereValues, value)
	}

	updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, strings.Join(updateFields, ", "), strings.Join(whereFields, " and "))
	logging.Debug(updateSQL)
	// 执行更新操作
	updateValues = append(updateValues, whereValues...)
	logging.DebugF("参数：%#v", updateValues)
	result, err := SqlDb.Exec(updateSQL, updateValues...)

	//sqlExec := fmt.Sprintf("UPDATE %s SET k_respara = \"%s\" WHERE k_sumalarmid = \"%s\"", tableName, infos.ResPara, infos.MainAlarmId)
	if err != nil {
		logging.Error("UPDATE recovertime error:" + err.Error())
		return 0, err
	}
	// 获取受影响的行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		logging.Error(err)
		return 0, err
	}
	logging.DebugF("Updated %d rows\n", rowsAffected)
	return rowsAffected, nil
}

func Query(SqlDb *sql.DB, sql string, describe TableDescribe) ([]map[string]interface{}, error) {
	rows, err := SqlDb.Query(sql)
	logging.Info("[Sql] Exec : " + sql)
	if err != nil {
		logging.Error("[Sql] Error : " + err.Error())
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnLength := len(columns)
	cache := make([]interface{}, columnLength) //临时存储每行数据
	for index, _ := range cache {              //为每一列初始化一个指针
		var a interface{}
		cache[index] = &a
	}

	var list []map[string]interface{} //返回的切片
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			d := *data.(*interface{})
			if d == nil {
				continue
			}
			//switch d.(type) {
			//case time.Time:
			//	item[columns[i]] = d.(time.Time)
			//default:
			//	if len(d.([]byte)) == 0 {
			//		continue
			//	}
			//	if b, ok := d.([]byte); ok {
			//		intValue, err3 := strconv.Atoi(string(b))
			//		if err3 != nil {
			//			item[columns[i]] = string(d.([]byte))
			//		} else {
			//			item[columns[i]] = intValue //取实际类型
			//		}
			//	}
			//}
			switch describe.Base[columns[i]] {
			case "int":
				if b, ok := d.([]byte); ok {
					intValue, err3 := strconv.Atoi(string(b))
					if err3 != nil {
						continue
					}
					item[columns[i]] = intValue //取实际类型
				}
			case "string", "[]byte":
				if len(d.([]byte)) == 0 {
					continue
				}
				if b, ok := d.([]byte); ok {
					item[columns[i]] = string(b)
				}
			case "float":
				if b, ok := d.([]byte); ok {
					floatNum, err4 := strconv.ParseFloat(string(b), 64)
					if err4 != nil {
						continue
					}
					item[columns[i]] = floatNum
				}
			case "time":
				item[columns[i]] = d.(time.Time)
			}
		}
		list = append(list, item)
	}

	_ = rows.Close()
	return list, nil
}

type TableDescribe struct {
	Base map[string]string
}

// TableInfo 表信息
type TableInfo struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   string
}

func DescribeTable(SqlDb *sql.DB, table string) (TableDescribe, error) {
	rows, err := SqlDb.Query("DESCRIBE " + table)
	if err != nil {
		return TableDescribe{}, err
	}
	fieldMap := make(map[string]string, 0)
	for rows.Next() {
		result := &TableInfo{}
		err = rows.Scan(&result.Field, &result.Type, &result.Null, &result.Key, &result.Default, &result.Extra)
		fiedlType := "null"
		if strings.Contains(result.Type, "int") {
			fiedlType = "int"
		}
		if strings.Contains(result.Type, "varchar") || strings.Contains(result.Type, "text") {
			fiedlType = "string"
		}
		if strings.Contains(result.Type, "float") || strings.Contains(result.Type, "doble") {
			fiedlType = "float"
		}
		if strings.Contains(result.Type, "blob") {
			fiedlType = "[]byte"
		}
		if strings.Contains(result.Type, "date") || strings.Contains(result.Type, "time") {
			fiedlType = "time"
		}
		fieldMap[result.Field] = fiedlType
	}
	_ = rows.Close()
	td := TableDescribe{
		Base: fieldMap,
	}
	return td, nil
}
