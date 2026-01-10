
#pragma once

#include "spdlog/spdlog.h"

#include <memory>
// #define DBCONNECTIONPOOL_LOG
// #define DBCONNECTIONPOOL_STATS_LOG
#include "StringUtils.h"

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
		bool _isArray;

	  public:
		Base() : _isNull(true), _isArray(false) {};
		virtual ~Base() = default;
		[[nodiscard]] bool isNull() const { return _isNull; };
		[[nodiscard]] bool isArray() const { return _isArray; }
	};

	template <typename T>
	class SqlType final : public Base
	{
		std::variant<T, std::vector<T>> value;

	  public:
		explicit SqlType(T v)
		{
			value = v;
			_isNull = false;
			_isArray = false;
		}
		explicit SqlType(const std::vector<T>& v)
		{
			value = v;
			_isNull = false;
			_isArray = true;
		}
		T as()
		{
			if (_isArray)
			{
				const std::string errorMessage = "Attempted to retrieve a single value from an array";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return std::get<T>(value);
		};
		std::vector<T> asArray()
		{
			if (!_isArray)
			{
				const std::string errorMessage = "Attempted to retrieve an array from a single value";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return std::get<std::vector<T>>(value);
		}
	};

	class SqlValue
	{
		std::shared_ptr<Base> value;

	  public:
		SqlValue() = default;
		~SqlValue() = default;

		void setValue(const std::shared_ptr<Base> &val) { this->value = val; };

		[[nodiscard]] bool isNull() const { return value->isNull(); };
		[[nodiscard]] bool isArray() const { return value->isArray(); }

		template <class T>
		T as(T valueIfNull = {})
		{
			if (isNull())
				return valueIfNull;
			if (isArray())
			{
				const std::string errorMessage = "SqlValue contains an array, use asArray<T>() instead";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			auto valued = dynamic_pointer_cast<SqlType<T>>(value);
			if (!valued)
			{
				const std::string errorMessage = "SqlValue type mismatch in as<T>()";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return valued->as();
		};

		template <class T>
		std::vector<T> asArray(std::vector<T> valueIfNull = {})
		{
			if (isNull())
				return valueIfNull;
			if (!isArray())
			{
				const std::string errorMessage = "SqlValue does not contain an array, use as<T>() instead";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			auto valued = std::dynamic_pointer_cast<SqlType<T>>(value);
			if (!valued)
			{
				const std::string errorMessage = "SqlValue type mismatch in asArray<T>()";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return valued->asArray();
		}

		template <class T>
		std::optional<T> asOpt()
		{
			if (isNull())
				return std::nullopt;
			if (isArray())
			{
				const std::string errorMessage = "SqlValue contains an array, use asArrayOpt<T>() instead";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			auto valued = dynamic_pointer_cast<SqlType<T>>(value);

			if (!valued)
			{
				const std::string errorMessage = "SqlValue type mismatch in as<T>()";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return valued->as();
		};

		template <class T>
		std::optional<std::vector<T>> asArrayOpt()
		{
			if (isNull())
				return std::nullopt;
			if (!isArray())
			{
				const std::string errorMessage = "SqlValue does not contain an array, use asOpt<T>() instead";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			auto valued = std::dynamic_pointer_cast<SqlType<T>>(value);
			if (!valued)
			{
				const std::string errorMessage = "SqlValue type mismatch in asArrayOpt<T>()";
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return valued->asArray();
		}

		template <typename T>
		std::vector<T> toVector(const pqxx::array<T> &arr)
		{
			std::vector<T> result;
			result.reserve(arr.size());
			for (auto it = arr.cbegin(); it != arr.cend(); ++it)
				result.push_back(static_cast<T>(*it)); // Copia sicura del valore
			/*
			Commentato per evitare un warning del compilatore generato a causa dell'operatore []
			for (int index = 0; index < arr.size(); index++)
				result.push_back(arr[index]); // Converte e copia
			*/
			return result;
		}
	};

	class SqlResultSet final
	{
	  public:
		SqlResultSet() = default;
		~SqlResultSet() = default;
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

		class SqlRow
		{
			friend class SqlResultSet;

		public:
			explicit SqlRow(std::vector<std::pair<std::string, SqlValueType>> *sqlColumnInfoByIndex,
				std::map<std::string, std::pair<size_t, SqlValueType>> *sqlColumnInfoByName
				)
				: _sqlColumnInfoByIndex(sqlColumnInfoByIndex), _sqlColumnInfoByName(sqlColumnInfoByName)
			{
			};
			~SqlRow() = default;

			void add(const SqlValue& sqlValue) { _sqlRow.push_back(sqlValue); }

			std::vector<SqlValue> &operator*() { return _sqlRow; }
			[[nodiscard]] std::pair<size_t, SqlValueType> & info(const std::string& columnName, const bool caseSensitive = false) const
			{
				const auto it = _sqlColumnInfoByName->find(caseSensitive ? columnName : StringUtils::lowerCase(columnName));
				if (it == _sqlColumnInfoByName->end())
				{
					const std::string errorMessage = std::format("Column name not found: {}", columnName);
					SPDLOG_ERROR(errorMessage);
					throw std::out_of_range(errorMessage);
				}
				return it->second;
			}
			[[nodiscard]] std::pair<std::string, SqlValueType> & info(const size_t columnIndex) const
			{
				if (columnIndex >= _sqlColumnInfoByIndex->size())
				{
					const std::string errorMessage = std::format("Wrong column index: {}", columnIndex);
					SPDLOG_ERROR(errorMessage);
					throw std::out_of_range(errorMessage);
				}
				return _sqlColumnInfoByIndex->at(columnIndex);
			}
			SqlValue &operator[](const size_t columnIndex)
			{
				if (columnIndex >= _sqlRow.size())
				{
					const std::string errorMessage = std::format("Wrong index: {}", columnIndex);
					SPDLOG_ERROR(errorMessage);
					throw std::out_of_range(errorMessage);
				}
				return _sqlRow[columnIndex];
			}
			SqlValue &operator[](const std::string& columnName)
			{
				return get(columnName);
			}
			SqlValue &get(const std::string& columnName, const bool caseSensitive = false)
			{
				return _sqlRow.at(info(columnName, caseSensitive).first);
			}
		private:
			std::vector<SqlValue> _sqlRow;
			// column Name / type per un accesso by Column Index
			std::vector<std::pair<std::string, SqlValueType>> *_sqlColumnInfoByIndex{};
			// type per un accesso by Column Name
			std::map<std::string, std::pair<size_t, SqlValueType>> *_sqlColumnInfoByName{};
		};

	private:
		// column Name / type per un accesso by Column Index
		std::vector<std::pair<std::string, SqlValueType>> _sqlColumnInfoByIndex;
		// type per un accesso by Column Name
		std::map<std::string, std::pair<size_t, SqlValueType>> _sqlColumnInfoByName;

		// numero di righe affette da una query di tipo INSERT, UPDATE, DELETE
		int affectedRows = 0;

		// per ogni riga (std::vector) abbiamo un vettore che contiene i valori delle colonne by Index
		// std::vector<std::vector<SqlValue>> _sqlValuesByIndex;
		std::vector<SqlRow> _sqlValuesByIndex;

		int32_t _count = 0;
		std::chrono::milliseconds _countSqlDuration = {};
		std::chrono::milliseconds _sqlDuration = {};

	  public:
		SqlRow buildSqlRow() { return SqlRow(&_sqlColumnInfoByIndex, &_sqlColumnInfoByName); };
		void clearData()
		{
			_sqlColumnInfoByIndex.clear();
			_sqlColumnInfoByName.clear();
			_sqlValuesByIndex.clear();
		};
		void addRow(const SqlRow& sqlRow) { _sqlValuesByIndex.push_back(sqlRow); };

		[[nodiscard]] size_t size() const { return _sqlValuesByIndex.size(); };
		[[nodiscard]] bool empty() const { return _sqlValuesByIndex.empty(); };
		void setAffectedRows(int affectedRows) { this->affectedRows = affectedRows; }
		[[nodiscard]] int getAffectedRows() const { return affectedRows; }
		nlohmann::json asJson();
		void addColumnType(std::string fieldName, SqlValueType sqlValueType)
		{
			size_t newColumnIndex = _sqlColumnInfoByIndex.size();
			auto it = _sqlColumnInfoByName.find(fieldName);
			if (it == _sqlColumnInfoByName.end())
				_sqlColumnInfoByName.insert(make_pair(fieldName, std::make_pair(newColumnIndex, sqlValueType)));
			else
				// se il nome della colonna è già presente, aggiungiamo anche l'indice della colonna
				_sqlColumnInfoByName.insert(make_pair(fmt::format("{} - {}", fieldName, newColumnIndex),
					std::make_pair(newColumnIndex, sqlValueType)));
			// _sqlColumnInfoByName.insert(make_pair(fieldName, sqlValueType));

			_sqlColumnInfoByIndex.emplace_back(fieldName, sqlValueType);
		};
		SqlValueType columnType(const std::string& columnName, const bool caseSensitive = false)
		{
			auto it = _sqlColumnInfoByName.find(caseSensitive ? columnName : StringUtils::lowerCase(columnName));
			if (it == _sqlColumnInfoByName.end())
			{
				const std::string errorMessage = std::format("Column name not found: {}", columnName);
				SPDLOG_ERROR(errorMessage);
				throw std::runtime_error(errorMessage);
			}
			return it->second.second;
		}
		[[nodiscard]] SqlValueType columnType(const size_t columnIndex) const
		{
			if (columnIndex >= _sqlColumnInfoByIndex.size())
			{
				const std::string errorMessage = std::format("Wrong column index: {}", columnIndex);
				SPDLOG_ERROR(errorMessage);
				throw std::out_of_range(errorMessage);
			}
			return _sqlColumnInfoByIndex[columnIndex].second;
		};
		std::string columnName(const size_t columnIndex)
		{
			if (columnIndex >= _sqlColumnInfoByIndex.size())
			{
				const std::string errorMessage = std::format("Wrong column index: {}", columnIndex);
				SPDLOG_ERROR(errorMessage);
				throw std::out_of_range(errorMessage);
			}
			return _sqlColumnInfoByIndex[columnIndex].first;
		};
		[[nodiscard]] size_t columnIndex(const std::string& columnName, const bool caseSensitive = false) const
		{
			auto it = _sqlColumnInfoByName.find(caseSensitive ? columnName : StringUtils::lowerCase(columnName));
			if (it == _sqlColumnInfoByName.end())
			{
				const std::string errorMessage = std::format("Column name not found: {}", columnName);
				SPDLOG_ERROR(errorMessage);
				throw std::out_of_range(errorMessage);
			}
			return it->second.first;
		}
		nlohmann::json asJson(const std::string& fieldName, SqlValue sqlValue);
		std::vector<SqlRow>::iterator begin() { return _sqlValuesByIndex.begin(); };
		std::vector<SqlRow>::iterator end() { return _sqlValuesByIndex.end(); };
		[[nodiscard]] std::vector<SqlRow>::const_iterator begin() const { return _sqlValuesByIndex.begin(); };
		[[nodiscard]] std::vector<SqlRow>::const_iterator end() const { return _sqlValuesByIndex.end(); };
		SqlRow &operator[](const int index)
		{
			if (index >= _sqlValuesByIndex.size())
			{
				const std::string errorMessage = std::format("Wrong index: {}", index);
				SPDLOG_ERROR(errorMessage);
				throw std::out_of_range(errorMessage);
			}
			return _sqlValuesByIndex[index];
		}

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
		const auto it = _sqlTablesColumnsSchema.find(tableName);
		if (it == _sqlTablesColumnsSchema.end())
			throw std::runtime_error(std::format("table {} not found", tableName));
		return it->second;
	}

	std::string getSqlColumnType(std::string tableName, const std::string& columnName)
	{
		std::map<std::string, std::shared_ptr<SqlColumnSchema>> sqlTableSchema = getSqlTableSchema(tableName);
		const auto it = sqlTableSchema.find(columnName);
		if (it == sqlTableSchema.end())
			throw std::runtime_error(std::format("column {}.{} not found", tableName, columnName));

		std::shared_ptr<SqlColumnSchema> sqlColumnSchema = it->second;

		return sqlColumnSchema->dataType;
	}

	std::string buildQueryColumns(const std::vector<std::string> &requestedColumns, bool convertDateFieldsToUtc = false);
	static std::shared_ptr<SqlResultSet> buildResult(const pqxx::result &result);

  private:
	std::map<std::string, std::map<std::string, std::shared_ptr<SqlColumnSchema>>> _sqlTablesColumnsSchema;

	static std::string getQueryColumn(
		const std::shared_ptr<SqlColumnSchema> &sqlColumnSchema, const std::string &requestedTableNameAlias,
		const std::string &requestedColumnName = "", bool convertDateFieldsToUtc = false
	);
	static std::string getColumnName(const std::shared_ptr<SqlColumnSchema> &sqlColumnSchema, const std::string &requestedTableNameAlias,
		std::string requestedColumnName);
	static bool isDataTypeManaged(const std::string &dataType, const std::string &arrayDataType);
};
