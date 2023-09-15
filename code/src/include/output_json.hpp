// ============================ //
// Thanks ChatGPT for the help! //
// ============================ //

#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class JsonOutput {
public:
    JsonOutput(const std::string& filename) : filename(filename) {
        // Open the JSON file for writing
        output_file.open(filename);
        if (!output_file.is_open()) {
            std::cerr << "Error opening JSON file: " << filename << std::endl;
        } else {
            // Start the JSON object
            // TODO - create the thing nicely with the time and other things
            json_output["data"] = json::array();
        }
    }

    ~JsonOutput() {
        // Close the JSON object and the file
        if (output_file.is_open()) {
            output_file << std::setw(4) << json_output << std::endl;
            output_file.close();
        }
    }

    void add_data(const json& obj) {
        // Add a JSON object to the array
        // TODO - create the thing nicely with the time and other things
        json_output["data"].push_back(obj);
    }

private:
    std::string filename;
    json json_output;
    std::ofstream output_file;
};
