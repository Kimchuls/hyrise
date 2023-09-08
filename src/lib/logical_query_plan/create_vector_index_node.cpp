#include "create_vector_index_node.hpp"

#include <sstream>

#include "static_table_node.hpp"

namespace hyrise {

CreateVectorIndexNode::CreateVectorIndexNode(const std::string& init_table_name, const std::string& init_index_name,
                                             const bool init_if_not_exists, 
                                             const std::vector<std::string> init_column_names,
                                             const TableVectorIndexDefinitions init_vector_index_definitions)
    : AbstractNonQueryNode(LQPNodeType::CreateVectorIndex),
      table_name(init_table_name),
      index_name(init_index_name),
      if_not_exists(init_if_not_exists),
      column_names(init_column_names),
      vector_index_definitions(init_vector_index_definitions) {}

    std::string CreateVectorIndexNode::description(const DescriptionMode /*mode*/) const {
      std::ostringstream stream;

      stream << "[CreateVectorIndexNode] " << (if_not_exists ? "IfNotExists " : "");
      stream << "TableName: '" << table_name << "'";
      stream << "IndexColumns: ";
      for (size_t iter = 0; iter < column_names.size(); iter++) {
        stream << "'" << column_names[iter] << "'";
      }
      stream << "IndexName: '" << index_name << "'";
      for (size_t iter = 0; iter < vector_index_definitions.size(); iter++) {
        TableVectorIndexDefinition vector_index_definition = vector_index_definitions[iter];
        stream << vector_index_definition.name << ": '" << vector_index_definition.value << "'";
      }

      return stream.str();
    }

    size_t CreateVectorIndexNode::_on_shallow_hash() const {
      auto hash = boost::hash_value(table_name);
      boost::hash_combine(hash, index_name);
      boost::hash_combine(hash, if_not_exists);
      for (size_t iter = 0; iter < column_names.size(); iter++) {
        boost::hash_combine(hash, column_names[iter]);
      }
      for (size_t iter = 0; iter < vector_index_definitions.size(); iter++) {
        TableVectorIndexDefinition vector_index_definition = vector_index_definitions[iter];
        boost::hash_combine(hash, vector_index_definition.name);
        boost::hash_combine(hash, vector_index_definition.value);
      }
      return hash;
    }

    std::shared_ptr<AbstractLQPNode> CreateVectorIndexNode::_on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const {
      return CreateVectorIndexNode::make(table_name, index_name, if_not_exists, column_names, vector_index_definitions);
    }

    bool CreateVectorIndexNode::_on_shallow_equals(const AbstractLQPNode& rhs,
                                                   const LQPNodeMapping& /*node_mapping*/) const {
      const auto& create_vector_index_node = static_cast<const CreateVectorIndexNode&>(rhs);
      //TODO: consider vector_index_definition & column_names equal
      return table_name == create_vector_index_node.table_name && index_name == create_vector_index_node.index_name &&
             if_not_exists == create_vector_index_node.if_not_exists;
    }

    }  // namespace hyrise
