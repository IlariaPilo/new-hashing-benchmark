// ============================ //
// Thanks ChatGPT for the help! //
// ============================ //
#pragma once

#include <iostream>
#include <fstream>
#include <ctime>
#include <unistd.h>
#include <limits.h>
#include <sys/sysinfo.h>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <omp.h>

#include <nlohmann/json.hpp>


using json = nlohmann::json;

class JsonOutput {
public:
    JsonOutput(const std::string& file_directory, const std::string& arg0, std::string filter = "", size_t thread_num = 1) {
        init(file_directory, arg0, filter, thread_num);
    }
    JsonOutput() = default;

    ~JsonOutput() {
        // Close the JSON object and the file
        if (output_file.is_open()) {
            output_file << std::setw(4) << json_output << std::endl;
            output_file.close();
        }
    }

    void add_data(const json& obj) {
        // Add a JSON object to the array
        #pragma omp critical
        {
            json_output["benchmarks"].push_back(obj);            
        }

    }

    void init(const std::string& file_directory, const std::string& arg0, std::string filter = "", size_t thread_num = 1) {
        // first, get current time
        std::time_t current_time = std::time(nullptr);
        std::tm* local_time = std::localtime(&current_time);
        // Format the date and time as "YYYY-MM-DD-HH-MM"
        std::stringstream ss;
        ss << std::put_time(local_time, "_%Y-%m-%d-%H-%M");
        // format the filter
        std::replace(filter.begin(), filter.end(), ',', '-');
        // define filename
        std::string filename = file_directory + "/" + filter + ss.str() + ".json";

        // Open the JSON file for writing
        output_file.open(filename);
        if (!output_file.is_open()) {
            throw std::runtime_error("Error opening JSON file.\n           [Hint!] Check that directory " + file_directory + " exists.\n");
        } else {
            // First, we make the context
            json_output["context"] = make_context(arg0, local_time, thread_num);
            // Then, we create the benchmark array
            json_output["benchmarks"] = json::array();
        }
    }

private:
    json json_output;
    std::ofstream output_file;

    json make_context(const std::string& arg0, std::tm* local_time, size_t thread_num) {
        json context;
        // -------------- date -------------- //
        // Get the time zone offset in minutes
        int time_zone = local_time->tm_gmtoff / 60;
        std::stringstream format_time;
        format_time << std::put_time(local_time, "%Y-%m-%dT%H:%M:%S");
        // Format the time zone offset
        format_time << std::showpos << std::internal << std::setw(3) << std::setfill('0')
                    << time_zone / 60 << ":" << std::noshowpos
                    << std::setw(2) << std::setfill('0') << time_zone % 60;
        context["date"] = format_time.str();

        char hostname[HOST_NAME_MAX];
        gethostname(hostname, HOST_NAME_MAX);
        context["host_name"] = hostname;
        context["executable"] = arg0;
        context["num_cpus"] = sysconf(_SC_NPROCESSORS_ONLN);
        context["num_threads"] = thread_num;
        //context["mhz_per_cpu"] = TODO, maybe;
        return context;
    }

};
