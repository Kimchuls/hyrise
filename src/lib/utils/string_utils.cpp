#include "string_utils.hpp"

#include <regex>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim_all.hpp>

namespace opossum {

std::vector<std::string> trim_and_split(const std::string& input) {
  auto converted = input;

  boost::algorithm::trim_all<std::string>(converted);
  auto arguments = std::vector<std::string>{};
  boost::algorithm::split(arguments, converted, boost::is_space());

  return arguments;
}

std::vector<std::string> split_string_by_delimiter(const std::string& str, const char delimiter) {
  auto internal = std::vector<std::string>{};
  auto sstream = std::stringstream(str);
  auto token = std::string{};

  while (std::getline(sstream, token, delimiter)) {
    internal.push_back(token);
  }

  return internal;
}

std::string plugin_name_from_path(const std::filesystem::path& path) {
  const auto filename = path.stem().string();

  // Remove "lib" prefix of shared library file
  auto plugin_name = filename.substr(3);

  return plugin_name;
}

std::string trim_source_file_path(const std::string& path) {
  const auto src_pos = path.find("/src/");
  if (src_pos == std::string::npos) {
    return path;
  }

  // "+ 1", since we want "src/lib/file.cpp" and not "/src/lib/file.cpp"
  return path.substr(src_pos + 1);
}

std::string replace_addresses(const std::string& input) {
  return std::regex_replace(input, std::regex{"0x[0-9A-Fa-f]{4,}"}, "0x00000000");
}

}  // namespace opossum
