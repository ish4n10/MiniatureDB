#pragma once
#include <string>

enum class CommandType {
	CREATE_TABLE,
	DROP_TABLE,
	INSERT,
	SELECT,
	DELETE,
	UPDATE,
	EXIT,
	UNKNOWN
};

struct Command {
	CommandType type = CommandType::UNKNOWN;
	std::string table_name;
	std::string key;
	std::string value;
};

