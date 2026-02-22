#pragma once
#include <variant>
#include <string>
#include "command.hpp"
#include <optional>
#include <cstdint>

enum class TokenType {
	KEYWORD,
	IDENTIFIER,
	VALUE,
	END,
	INVALID
};

struct Token {
	TokenType type;
	std::variant<std::string, int64_t, double> value;
};


class Tokenizer {
private:
	std::string input;
	int input_length;
	int current_pos = 0;

public:
	Tokenizer(const std::string& input) : input(input), input_length(static_cast<int>(input.size())) {}
	
	bool has_next() const;
	std::string next_token();
	
	std::vector<
};