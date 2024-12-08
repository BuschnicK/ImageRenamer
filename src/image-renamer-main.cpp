#include <SDKDDKVer.h>

#include "boost/program_options.hpp"

#include <atomic>
#include <condition_variable>
#include <exception>
#include <fstream>
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

enum SegmentMarker : std::uint16_t {
  kJpegHeader = 0xffd8,
  kApp1Header = 0xffe1,
  kTiffByteOrderLittleEndian = 0x4949,  // "II"
  kTiffByteOrderBigEndian = 0x4d4d,  // "MM"
};

// There are many EXIF tags. We only care about a handful.
// https://exiftool.org/TagNames/EXIF.html (https://exiftool.org/htmldump.html)
// https://exiv2.org/tags.html
enum ExifTags : std::uint16_t {
  kExifOffset = 0x8769,
  kDateTimeOriginal = 0x9003,
  kTimeZoneOffset = 0x882a,
};

// http://www.fifi.org/doc/jhead/exif-e.html
enum ExifFormat : std::uint16_t {
  kUInt8 = 1,
  kAscii = 2,
  kUInt16 = 3,
  kUInt32 = 4,
  kURational = 5,
  kSInt8 = 6,
  kUndefined = 7,
  kSInt16 = 8,
  kSInt32 = 9,
  kSRational = 10,
  kFloat = 11,
  kDouble = 12,
};

int GetSize(const ExifFormat format) {
  switch (format) {
    case kUInt8:
    case kSInt8:
    case kAscii:
    case kUndefined: 
      return 1;
    case kUInt16:
    case kSInt16: 
      return 2; 
    case kUInt32:
    case kSInt32:
    case kFloat: 
      return 4;
    case kURational:
    case kSRational:
    case kDouble:
      return 8;
  }
  throw std::invalid_argument("Invalid EXIF format tag.");
}

// Image File Directory entry. Each entry is 12 bytes.
// - 2 bytes tag number
// - 2 bytes data format
// - 4 bytes number of components
// - 4 bytes data or offset to data
struct IfdEntry {
  std::uint16_t tag;
  std::uint16_t format;
  std::uint32_t num_components;
  union {
    std::uint32_t data;
    std::uint32_t offset;
  } payload;
};

void Expect(bool expectation, std::string_view message = "") {
  if (!expectation) {
    throw std::runtime_error(
        boost::str(boost::format("Expectation failed: %s") % message));
  }
}

std::uint16_t ReadWord(
    std::istream& stream,
    SegmentMarker byte_order = SegmentMarker::kTiffByteOrderBigEndian) {
  std::uint16_t word;
  stream.read(reinterpret_cast<char*>(&word), sizeof(word));
  if (!stream) {
    throw std::runtime_error("Failed reading file.");
  }
  return byte_order == SegmentMarker::kTiffByteOrderBigEndian
             ? _byteswap_ushort(word)
             : word;
}

std::uint32_t ReadDoubleWord(
    std::istream& stream,
    SegmentMarker byte_order = SegmentMarker::kTiffByteOrderBigEndian) {
  std::uint32_t dword;
  stream.read(reinterpret_cast<char*>(&dword), sizeof(dword));
  if (!stream) {
    throw std::runtime_error("Failed reading file.");
  }
  return byte_order == SegmentMarker::kTiffByteOrderBigEndian
             ? _byteswap_ulong(dword)
             : dword;
}

void ReadBytes(std::istream& stream, char* destination, int size) {
  stream.read(destination, size);
  if (!stream) {
    throw std::runtime_error("Failed reading file.");
  }
}

IfdEntry ReadIfdEntry(std::istream& stream, const SegmentMarker byte_order) {
  IfdEntry entry {
    .tag = ReadWord(stream, byte_order), 
    .format = ReadWord(stream, byte_order),
    .num_components = ReadDoubleWord(stream, byte_order),
    .payload = ReadDoubleWord(stream, byte_order),
  };
  /*  
  // Format determines the size of a single component. Multiplying that
  // with the number of components gives us the size. This in turn determines
  // whether the value is stored or an offset to the value.
  const int total_size =
      GetSize(static_cast<ExifFormat>(entry.format)) * entry.num_components;
  if (total_size > 4) {
  }
  */
  return entry;
}

void ReadExifData(std::string_view filename) { 
  std::ifstream file(filename.data(),
                     std::ios_base::in | std::ios_base::binary);
  Expect(ReadWord(file) == SegmentMarker::kJpegHeader, "Missing JPEG header.");
  Expect(ReadWord(file) == SegmentMarker::kApp1Header, "Missing APP1 header.");
  const std::uint16_t app1_size = ReadWord(file);

  char exif_header[6];
  ReadBytes(file, exif_header, 6);
  Expect(strncmp(exif_header, "Exif\0\0", 6) == 0, "Missing EXIF header.");

  const SegmentMarker tiff_byte_order =
      static_cast<SegmentMarker>(ReadWord(file));
  Expect(tiff_byte_order == SegmentMarker::kTiffByteOrderBigEndian ||
             tiff_byte_order == SegmentMarker::kTiffByteOrderLittleEndian,
         "Unexpected TIFF byte order marker.");
  Expect(ReadWord(file, tiff_byte_order) == 42, "Unexpected TIFF byte order control value.");

  // The offset to the first image file descriptor. From the beginning of 
  // the TIFF file, so in our case starting at the TIFF header.
  const std::uint32_t ifd0_offset = ReadDoubleWord(file, tiff_byte_order);
  Expect(file.seekg(ifd0_offset - 8, std::ios_base::cur).good(),
         "Invalid IFD0 offset.");
  const std::uint16_t num_ifd0_entries = ReadWord(file, tiff_byte_order);
  std::vector<IfdEntry> entries;
  for (int i = 0; i < num_ifd0_entries; ++i) {
    IfdEntry entry = ReadIfdEntry(file, tiff_byte_order);
    // Only remember the entries we care about.
    switch (entry.tag) {
      case kExifOffset:
      case kDateTimeOriginal:
      case kTimeZoneOffset:
        entries.push_back(std::move(entry));
    }
  }

}

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
  auto work_guard = boost::asio::make_work_guard(io_context);
  boost::thread_group threads;
  for (std::size_t i = 0; i < std::thread::hardware_concurrency(); ++i) {
    threads.create_thread(
        boost::bind(&boost::asio::io_context::run, &io_context));
  }

  std::atomic<int> num_processed_successfully = 0;
  std::atomic<int> num_failed = 0;  
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
    std::unique_lock<std::mutex> lock(mutex);
    // Rate limit the work queue to twice the number of tasks as threads.
    while (num_in_progress >= threads.size() * 2) {
       busy.wait(lock);
    }
    ++num_in_progress;

    boost::asio::post(io_context, [entry, output_dir, &num_in_progress,
                                   &num_processed_successfully,
                                   &num_failed, &busy]() {
      try {
        std::osyncstream(std::cout) << "Reading: " << entry << std::endl;
        ReadExifData(entry.path().string());
        --num_in_progress;
        ++num_processed_successfully;
        busy.notify_one();
      } catch (const std::exception& error) {
        std::osyncstream(std::cerr)
            << "error: " << entry.path().string() << " " << error.what() << std::endl;
        --num_in_progress;
        ++num_failed;
        busy.notify_one();
      }
    });
  }

  work_guard.reset();
  threads.join_all();

  std::cout << num_processed_successfully << " succeeded, " << num_failed
            << " failed" << std::endl;
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
