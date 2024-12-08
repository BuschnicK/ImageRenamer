#include <SDKDDKVer.h>

#include "boost/program_options.hpp"

#include <atomic>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <mutex>
#include <optional>
#include <string_view>
#include <string>
#include <syncstream>
#include <thread>

#include "boost/algorithm/string/case_conv.hpp"
#include "boost/asio.hpp"
#include "boost/filesystem.hpp"
#include "boost/format.hpp"
#include "boost/nowide/fstream.hpp"
#include "boost/program_options.hpp"
#include "boost/thread/thread.hpp"

namespace {

void Main(std::string_view input_dir,
          std::optional<std::string_view> output_dir_string) {
  if (!output_dir_string.has_value()) {
    output_dir_string = input_dir;
  }
  const boost::filesystem::path output_dir(output_dir_string->data());
  if (!boost::filesystem::is_directory(output_dir)) {
    throw std::invalid_argument(boost::str(
        boost::format("Not a directory: \"%s\"") % *output_dir_string));
  }

  boost::asio::io_context io_context;
  boost::thread_group threads;
  for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
    threads.create_thread(
        boost::bind(&boost::asio::io_context::run, &io_context));
  }

  std::atomic<int> num_in_progress = 0;
  std::mutex mutex;
  std::condition_variable busy;
  for (boost::filesystem::directory_entry& entry :
       boost::filesystem::directory_iterator(input_dir.data())) {
    if (!boost::filesystem::is_regular_file(entry)) {
      continue;
    }
    const std::string extension =
        boost::algorithm::to_lower_copy(entry.path().extension().string());
    if (extension != ".jpg" && extension != ".jpeg") {
      continue;
    }
    std::osyncstream(std::cout) << "Reading: " << entry << std::endl;

    std::unique_lock<std::mutex> lock(mutex);
    // Rate limit the work queue to twice the number of tasks as threads.
    while (num_in_progress >= threads.size() * 2) {
      busy.wait(lock);
    }
    ++num_in_progress;

    boost::asio::post(io_context, [entry, output_dir, &num_in_progress, &busy]() {
      try {
        --num_in_progress;
        busy.notify_one();
      } catch (const std::exception& error) {
        std::osyncstream(std::cerr) << "error: " << error.what() << std::endl;
        --num_in_progress;
        busy.notify_one();
      }
    });
  }

  io_context.stop();
  threads.join_all();
}

}  // namespace

int main(int argc, char** argv) {
  try {
    boost::program_options::options_description flags_description(
        "Supported options");
    flags_description.add_options()("help", "List command line options")(
        "input_dir", boost::program_options::value<std::string>(),
        "Input directory containing JPEG files.")(
        "output_dir", boost::program_options::value<std::string>(),
        "Output directory. Defaults to input_dir.");

    boost::program_options::variables_map flags;
    boost::program_options::store(boost::program_options::parse_command_line(
                                      argc, argv, flags_description),
                                  flags);
    boost::program_options::notify(flags);

    if (flags.count("help") || flags.empty()) {
      std::cout << flags_description << std::endl;
      return EXIT_SUCCESS;
    }
    if (!flags.count("input_dir")) {
      std::cout << "input_dir must be provided!\n";
      std::cout << flags_description << std::endl;
      return EXIT_FAILURE;
    }
    std::optional<std::string> output_dir;
    if (flags.contains("output_dir")) {
      output_dir = flags["output_dir"].as<std::string>();
    }
    Main(flags["input_dir"].as<std::string>(), output_dir);
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << std::endl;
    return EXIT_FAILURE;
  } catch (...) {
    std::cerr << "Unknown error." << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
