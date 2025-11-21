
#pragma once

#include "spdlog/spdlog.h"

#include <memory>
// #define DBCONNECTIONPOOL_LOG
// #define DBCONNECTIONPOOL_STATS_LOG
#include <JSONUtils.h>
#include <PostgresConnection.h>
#include <string>
#include <utility>

class PostgresHelper
{
  public:
	struct SqlColumnSchema
	{
		SqlColumnSchema(string tableName, string columnName, bool nullable, string dataType, string arrayDataType)
		{
			this->tableName = std::move(tableName);
			this->columnName = std::move(columnName);
			this->nullable = nullable;
			this->dataType = std::move(dataType);
			this->arrayDataType = std::move(arrayDataType);
		}

		string tableName;
		string columnName;
		bool nullable;
		string dataType;
		string arrayDataType;
	};

  public:
	class Base
	{
	  protected:
		bool _isNull;

	  public:
		Base() { _isNull = true; };
		virtual ~Base() = default;
		[[nodiscard]] bool isNull() const { return _isNull; };
	};

	template <typename T> class SqlType final : public Base
	{
		T value;

	  public:
		explicit SqlType(T v)
		{
			value = v;
			_isNull = false;
		};
		T as() { return value; };
	};

	class SqlValue
	{
		shared_ptr<Base> value;

	  public:
		SqlValue() = default;
		~SqlValue() = default;

		void setValue(const shared_ptr<Base> &val) { this->value = val; };

		[[nodiscard]] bool isNull() const { return value->isNull(); };

		template <class T> T as(T valueIfNull)
		{
			if (isNull())
				return valueIfNull;
			auto valued = dynamic_pointer_cast<SqlType<T>>(value);

			if (!valued)
			{
				const string errorMessage = "SqlValue type mismatch in as<T>()";
				SPDLOG_ERROR(errorMessage);
				throw runtime_error(errorMessage);
			}
			return valued->as();
		};
	};

	class SqlResultSet final
	{
	  public:
		virtual ~SqlResultSet() = default;
		enum SqlValueType
		{
			unknown,
			int16,
			int32,
			int64,
			double_,
			text,
			boolean,
			json_,
			vectorInt32,
			vectorInt64,
			vectorDouble,
			vectorText,
			vectorBoolean
		};

	  private:
		// column Name / type
		vector<pair<string, SqlValueType>> _sqlColumnTypeByIndex;
		map<string, SqlValueType> _sqlColumnTypeByName;

		// per ogni riga (vector) abbiamo un vettore che contiene i valori delle colonne by Index
		vector<vector<SqlValue>> _sqlValuesByIndex;

		int32_t _count = 0;
		chrono::milliseconds _countSqlDuration = {};
		chrono::milliseconds _sqlDuration = {};

		// temporary vector to fill _sqlValuesByIndex
		vector<SqlValue> _sqlCurrentRowValuesByIndex;

	  public:
		virtual void clearData()
		{
			_sqlColumnTypeByIndex.clear();
			_sqlColumnTypeByName.clear();
			_sqlValuesByIndex.clear();
		};
		virtual void addColumnValueToCurrentRow(const string& fieldName, const SqlValue& sqlValue) { _sqlCurrentRowValuesByIndex.push_back(sqlValue); };
		virtual void addCurrentRow()
		{
			_sqlValuesByIndex.push_back(_sqlCurrentRowValuesByIndex);
			_sqlCurrentRowValuesByIndex.clear();
		};
		virtual size_t size() { return _sqlValuesByIndex.size(); };
		virtual bool empty() { return _sqlValuesByIndex.empty(); };
		virtual json asJson();
		void addColumnType(string fieldName, SqlValueType sqlValueType)
		{
			auto it = _sqlColumnTypeByName.find(fieldName);
			if (it == _sqlColumnTypeByName.end())
				_sqlColumnTypeByName.insert(make_pair(fieldName, sqlValueType));
			else
				// se il nome della colonna è già presente, aggiungiamo anche l'indice della colonna
				_sqlColumnTypeByName.insert(make_pair(fmt::format("{} - {}", fieldName, _sqlColumnTypeByIndex.size()), sqlValueType));
			_sqlColumnTypeByName.insert(make_pair(fieldName, sqlValueType));

			_sqlColumnTypeByIndex.emplace_back(fieldName, sqlValueType);
		};
		SqlValueType type(const string& fieldName);
		json asJson(const string& fieldName, SqlValue sqlValue);
		string getColumnNameByIndex(const size_t columnIndex) { return _sqlColumnTypeByIndex[columnIndex].first; };
		[[nodiscard]] size_t getColumnIndexByName(const string& columnName) const
		{
			for (size_t index = 0, size = _sqlColumnTypeByIndex.size(); index < size; index++)
				if (_sqlColumnTypeByIndex[index].first == columnName)
					return index;
			return -1;
		};
		vector<vector<SqlValue>>::iterator begin() { return _sqlValuesByIndex.begin(); };
		vector<vector<SqlValue>>::iterator end() { return _sqlValuesByIndex.end(); };
		[[nodiscard]] vector<vector<SqlValue>>::const_iterator begin() const { return _sqlValuesByIndex.begin(); };
		[[nodiscard]] vector<vector<SqlValue>>::const_iterator end() const { return _sqlValuesByIndex.end(); };
		vector<SqlValue> &operator[](int index) { return _sqlValuesByIndex[index]; }

		void setCount(int32_t count) { _count = count; }
		[[nodiscard]] int32_t getCount() const { return _count; }
		void setCountSqlDuration(chrono::milliseconds countSqlDuration) { _countSqlDuration = countSqlDuration; }
		[[nodiscard]] chrono::milliseconds getCountSqlDuration() const { return _countSqlDuration; }
		void setSqlDuration(chrono::milliseconds sqlDuration) { _sqlDuration = sqlDuration; }
		[[nodiscard]] chrono::milliseconds getSqlDuration() const { return _sqlDuration; }
	};

  public:
	PostgresHelper();
	~PostgresHelper();
	void loadSqlColumnsSchema(PostgresConnTrans &trans);
	map<string, shared_ptr<SqlColumnSchema>> getSqlTableSchema(const string& tableName)
	{
		auto it = _sqlTablesColumnsSchema.find(tableName);
		if (it == _sqlTablesColumnsSchema.end())
			throw runtime_error(std::format("table {} not found", tableName));
		return it->second;
	}

	string getSqlColumnType(string tableName, const string& columnName)
	{
		map<string, shared_ptr<SqlColumnSchema>> sqlTableSchema = getSqlTableSchema(tableName);
		auto it = sqlTableSchema.find(columnName);
		if (it == sqlTableSchema.end())
			throw runtime_error(std::format("column {}.{} not found", tableName, columnName));

		shared_ptr<SqlColumnSchema> sqlColumnSchema = it->second;

		return sqlColumnSchema->dataType;
	}

	string buildQueryColumns(const vector<string> &requestedColumns, bool convertDateFieldsToUtc = false);
	static shared_ptr<PostgresHelper::SqlResultSet> buildResult(const result &result);

  private:
	map<string, map<string, shared_ptr<SqlColumnSchema>>> _sqlTablesColumnsSchema;

	static string getQueryColumn(
		const shared_ptr<SqlColumnSchema> &sqlColumnSchema, const string &requestedTableNameAlias, const string &requestedColumnName = "",
		bool convertDateFieldsToUtc = false
	);
	static string getColumnName(const shared_ptr<SqlColumnSchema> &sqlColumnSchema, const string &requestedTableNameAlias, string requestedColumnName);
	static bool isDataTypeManaged(const string &dataType, const string &arrayDataType);
};
