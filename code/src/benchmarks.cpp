#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <unistd.h>

/* ======== options ======== */
    std::string input_dir = "";
    std::string output_dir = "";
    int threads;
    std::string filter = "";
/* ========================= */

// Function to print the usage information
void show_usage() {
    std::cout << "\n\033[1;96m./benchmarks [ARGS]\033[0m" << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  -i, --input INPUT_DIR     Directory storing the datasets" << std::endl;
    std::cout << "  -o, --output OUTPUT_DIR   Directory that will store the output" << std::endl;
    std::cout << "  -t, --threads THREADS     Number of threads to use (default: all)" << std::endl;
    std::cout << "  -f, --filter FILTER       Type of benchmarks to execute (default: all)" << std::endl;
    std::cout << "  -h, --help                Display this help message\n" << std::endl;
}
int pars_args(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            show_usage();
            return 1;
        }
        if (arg == "--input" || arg == "-i") {
            if (i + 1 < argc) {
                input_dir = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --input requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--output" || arg == "-o") {
            if (i + 1 < argc) {
                output_dir = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --output requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--threads" || arg == "-t") {
            if (i + 1 < argc) {
                threads = std::stoi(argv[i + 1]);
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --threads requires an argument." << std::endl;
                return 2;
            }
        }
        if (arg == "--filter" || arg == "-f") {
            if (i + 1 < argc) {
                filter = argv[i + 1];
                i++; // Skip the next argument
                continue;
            } else {
                std::cerr << "Error: --filter requires an argument." << std::endl;
                return 2;
            }
        }
        // if we are here, then the option is unknown 
        std::cerr << "Error: Unknown option " << arg << std::endl;
        show_usage();
        return 2;
    }
    return 0;
}

int main(int argc, char* argv[]) {
    // get thread number
    threads = sysconf(_SC_NPROCESSORS_ONLN);
    // Parse command-line arguments
    int do_exit = pars_args(argc, argv);
    if (do_exit)
        return do_exit-1;
    // Check if mandatory options are provided
    if (input_dir == "" || output_dir == "") {
        std::cerr << "Error: Mandatory options --input and --output must be provided." << std::endl;
        show_usage();
        return 1;
    }


    return 0;
}
