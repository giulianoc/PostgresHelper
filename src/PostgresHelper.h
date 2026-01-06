
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
		SqlColumnSchema(std::string tableName, std::string columnName, bool nullable, std::string dataType, std::string arrayDataType)
		{
			this->tableName = std::move(tableName);
			this->columnName = std::move(columnName);
			this->nullable = nullable;
			this->dataType = std::move(dataType);
			this->arrayDataType = std::move(arrayDataType);
		}

		std::string tableName;
		std::string columnName;
		bool nullable;
		std::string dataType;
		std::string arrayDataType;
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
		std::shared_ptr<Base> value;

	  public:
		SqlValue() = default;
		~SqlValue() = default;

		void setValue(const std::shared_ptr<Base> &val) { this->value = val; };

		[[nodiscard]] bool isNull() const { return value->isNull(); };

		template <class T> T as(T valueIfNull = {})
		{
			if (isNull())
				return valueIfNull;
			auto valued = dynamic_pointer_cast<SqlType<T>>(value);

			if (!valued)
			{
				const std::string errorMessage = "SqlValue type mismatch in as<T>()";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
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
		std::vector<std::pair<std::string, SqlValueType>> _sqlColumnTypeByIndex;
		std::map<std::string, SqlValueType> _sqlColumnTypeByName;

		// per ogni riga (std::vector) abbiamo un vettore che contiene i valori delle colonne by Index
		std::vector<std::vector<SqlValue>> _sqlValuesByIndex;

		int32_t _count = 0;
		std::chrono::milliseconds _countSqlDuration = {};
		std::chrono::milliseconds _sqlDuration = {};

		// temporary std::vector to fill _sqlValuesByIndex
		std::vector<SqlValue> _sqlCurrentRowValuesByIndex;

	  public:
		virtual void clearData()
		{
			_sqlColumnTypeByIndex.clear();
			_sqlColumnTypeByName.clear();
			_sqlValuesByIndex.clear();
		};
		virtual void addColumnValueToCurrentRow(const std::string& fieldName, const SqlValue& sqlValue) { _sqlCurrentRowValuesByIndex.push_back(sqlValue); };
		virtual void addCurrentRow()
		{
			_sqlValuesByIndex.push_back(_sqlCurrentRowValuesByIndex);
			_sqlCurrentRowValuesByIndex.clear();
		};
		virtual size_t size() { return _sqlValuesByIndex.size(); };
		virtual bool empty() { return _sqlValuesByIndex.empty(); };
		virtual nlohmann::json asJson();
		void addColumnType(std::string fieldName, SqlValueType sqlValueType)
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
		SqlValueType type(const std::string& fieldName);
		nlohmann::json asJson(const std::string& fieldName, SqlValue sqlValue);
		std::string getColumnNameByIndex(const size_t columnIndex) { return _sqlColumnTypeByIndex[columnIndex].first; };
		[[nodiscard]] SqlValueType getColumnTypeByIndex(const size_t columnIndex) const { return _sqlColumnTypeByIndex[columnIndex].second; };
		[[nodiscard]] size_t getColumnIndexByName(const std::string& columnName) const
		{
			for (size_t index = 0, size = _sqlColumnTypeByIndex.size(); index < size; index++)
				if (_sqlColumnTypeByIndex[index].first == columnName)
					return index;
			return -1;
		};
		std::vector<std::vector<SqlValue>>::iterator begin() { return _sqlValuesByIndex.begin(); };
		std::vector<std::vector<SqlValue>>::iterator end() { return _sqlValuesByIndex.end(); };
		[[nodiscard]] std::vector<std::vector<SqlValue>>::const_iterator begin() const { return _sqlValuesByIndex.begin(); };
		[[nodiscard]] std::vector<std::vector<SqlValue>>::const_iterator end() const { return _sqlValuesByIndex.end(); };
		std::vector<SqlValue> &operator[](const int index) { return _sqlValuesByIndex[index]; }

		void setCount(int32_t count) { _count = count; }
		[[nodiscard]] int32_t getCount() const { return _count; }
		void setCountSqlDuration(std::chrono::milliseconds countSqlDuration) { _countSqlDuration = countSqlDuration; }
		[[nodiscard]] std::chrono::milliseconds getCountSqlDuration() const { return _countSqlDuration; }
		void setSqlDuration(std::chrono::milliseconds sqlDuration) { _sqlDuration = sqlDuration; }
		[[nodiscard]] std::chrono::milliseconds getSqlDuration() const { return _sqlDuration; }
	};

  public:
	PostgresHelper();
	~PostgresHelper();
	void loadSqlColumnsSchema(PostgresConnTrans &trans);
	std::map<std::string, std::shared_ptr<SqlColumnSchema>> getSqlTableSchema(const std::string& tableName)
	{
		auto it = _sqlTablesColumnsSchema.find(tableName);
		if (it == _sqlTablesColumnsSchema.end())
			throw std::runtime_error(std::format("table {} not found", tableName));
		return it->second;
	}

	std::string getSqlColumnType(std::string tableName, const std::string& columnName)
	{
		std::map<std::string, std::shared_ptr<SqlColumnSchema>> sqlTableSchema = getSqlTableSchema(tableName);
		auto it = sqlTableSchema.find(columnName);
		if (it == sqlTableSchema.end())
			throw std::runtime_error(std::format("column {}.{} not found", tableName, columnName));

		std::shared_ptr<SqlColumnSchema> sqlColumnSchema = it->second;

		return sqlColumnSchema->dataType;
	}

	std::string buildQueryColumns(const std::vector<std::string> &requestedColumns, bool convertDateFieldsToUtc = false);
	static std::shared_ptr<PostgresHelper::SqlResultSet> buildResult(const pqxx::result &result);

  private:
	std::map<std::string, std::map<std::string, std::shared_ptr<SqlColumnSchema>>> _sqlTablesColumnsSchema;

	static std::string getQueryColumn(
		const std::shared_ptr<SqlColumnSchema> &sqlColumnSchema, const std::string &requestedTableNameAlias, const std::string &requestedColumnName = "",
		bool convertDateFieldsToUtc = false
	);
	static std::string getColumnName(const std::shared_ptr<SqlColumnSchema> &sqlColumnSchema, const std::string &requestedTableNameAlias, std::string requestedColumnName);
	static bool isDataTypeManaged(const std::string &dataType, const std::string &arrayDataType);
};
