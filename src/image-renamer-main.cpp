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
#include "boost/date_time/local_time/local_time_io.hpp"
#include "boost/filesystem.hpp"
#include "boost/format.hpp"
#include "boost/nowide/fstream.hpp"
#include "boost/program_options.hpp"
#include "boost/thread/thread.hpp"

namespace {

enum SegmentMarker : std::uint16_t {
  kJpegHeader = 0xffd8,
  kApp0Header = 0xffe0,
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
  kOffsetTimeOriginal = 0x9011,
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
  throw std::invalid_argument(
      boost::str(boost::format("Invalid EXIF format tag %i.") % format));
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

bool IsDataInlined(const IfdEntry& entry) {
  // Format determines the size of a single component. Multiplying that
  // with the number of components gives us the size. This in turn determines
  // whether the value is stored directly or an offset to the value.
  return GetSize(static_cast<ExifFormat>(entry.format)) *
             entry.num_components <=
         4;
}

boost::local_time::local_date_time ParseTime(
    std::string_view date_string, std::string_view timezone_offset_string) {
  boost::local_time::local_time_input_facet* input_facet =
      new boost::local_time::local_time_input_facet();
  input_facet->format("%Y:%m:%d %H:%M:%S%Q");
  std::stringstream datetime_stream;
  datetime_stream.imbue(std::locale(datetime_stream.getloc(), input_facet));
  datetime_stream << date_string << timezone_offset_string;
  boost::local_time::local_date_time time(boost::date_time::not_a_date_time);
  datetime_stream >> time;
  if (time.is_not_a_date_time()) {
    throw std::invalid_argument(boost::str(
        boost::format("Not a valid date time: \"%s\"") % datetime_stream.str()));
  }
  return time;
}

std::string FormatTime(const boost::local_time::local_date_time& time) {
  boost::local_time::local_time_facet* facet =
      new boost::local_time::local_time_facet("%Y-%m-%d %H-%M-%S");
  std::ostringstream datetime_stream;
  datetime_stream.imbue(std::locale(datetime_stream.getloc(), facet));
  datetime_stream << time;
  return datetime_stream.str();
}

IfdEntry ReadIfdEntry(std::istream& stream, const SegmentMarker byte_order) {
  IfdEntry entry {
    .tag = ReadWord(stream, byte_order), 
    .format = ReadWord(stream, byte_order),
    .num_components = ReadDoubleWord(stream, byte_order),
    .payload = ReadDoubleWord(stream, byte_order),
  };
  return entry;
}

boost::local_time::local_date_time ReadExifData(std::string_view filename) {
  std::ifstream file(filename.data(),
                     std::ios_base::in | std::ios_base::binary);
  int num_header_bytes = 0xc;  // Default to JPEG, APP1, EXIF headers
  Expect(ReadWord(file) == SegmentMarker::kJpegHeader, "Missing JPEG header.");
  std::uint16_t app_header = ReadWord(file);
  if (app_header == SegmentMarker::kApp0Header) {
    // Skip App0 header.
    const std::uint16_t app0_size = ReadWord(file);
    // The size excludes the two header bytes.
    Expect(
        file.seekg(app0_size - 2, std::ios_base::cur).good(),
        "Invalid APP0 size.");
    num_header_bytes += app0_size + 2;
    app_header = ReadWord(file);
  }
  Expect(app_header == SegmentMarker::kApp1Header, "Missing APP1 header.");
  const std::uint16_t app1_size = ReadWord(file);

  char exif_header[6];
  ReadBytes(file, exif_header, 6);
  Expect(strncmp(exif_header, "Exif\0\0", 6) == 0, "Missing EXIF header.");

  const SegmentMarker tiff_byte_order =
      static_cast<SegmentMarker>(ReadWord(file));
  Expect(tiff_byte_order == SegmentMarker::kTiffByteOrderBigEndian ||
             tiff_byte_order == SegmentMarker::kTiffByteOrderLittleEndian,
         "Unexpected TIFF byte order marker.");
  Expect(ReadWord(file, tiff_byte_order) == 42,
         "Unexpected TIFF byte order control value.");

  // The offset to the first image file descriptor. From the beginning of
  // the TIFF file, so in our case starting at the TIFF header.
  const std::uint32_t ifd0_offset = ReadDoubleWord(file, tiff_byte_order);
  Expect(file.seekg(ifd0_offset - 8, std::ios_base::cur).good(),
         "Invalid IFD0 offset.");
  const std::uint16_t num_ifd0_entries = ReadWord(file, tiff_byte_order);
  std::vector<IfdEntry> entries;
  for (int i = 0; i < num_ifd0_entries; ++i) {
    IfdEntry entry = ReadIfdEntry(file, tiff_byte_order);
    switch (entry.tag) {
      case kExifOffset:
      case kDateTimeOriginal:
      case kTimeZoneOffset:
      case kOffsetTimeOriginal:
        entries.push_back(std::move(entry));
    }
  }
  for (const IfdEntry& ifd_entry : entries) {
    if (ifd_entry.tag == kExifOffset) {
      Expect(file.seekg(ifd_entry.payload.offset + num_header_bytes,
                        std::ios_base::beg)
                 .good(),
             "Invalid IFD offset.");
      break;
    }
  }
  // Read the IFD:
  const std::uint16_t num_ifd_entries = ReadWord(file, tiff_byte_order);
  for (int i = 0; i < num_ifd_entries; ++i) {
    IfdEntry entry = ReadIfdEntry(file, tiff_byte_order);
    switch (entry.tag) {
      case kDateTimeOriginal:
      case kTimeZoneOffset:
      case kOffsetTimeOriginal:
        entries.push_back(std::move(entry));
    }
  }
  std::string date_string;
  std::string time_zone_offset;
  for (const IfdEntry& ifd_entry : entries) {
    switch (ifd_entry.tag) {
      case kDateTimeOriginal: {
        Expect(!IsDataInlined(ifd_entry),
               "Datetime is expected to be stored at an offset.");
        // Offsets are relative to the beginning of the exiff file, so we need 
        // to skip JPEG, APP0, APP1 and EXIF headers bytes.
        Expect(file.seekg(ifd_entry.payload.offset + num_header_bytes, std::ios_base::beg)
                   .good(),
            "Invalid DatetimeOriginal offset.");
        date_string =
            std::string(std::min<size_t>(ifd_entry.num_components, 100),
                                '\0');
        ReadBytes(file, &date_string[0], static_cast<int>(date_string.size()));
      } break;
      case kOffsetTimeOriginal: {
        time_zone_offset = std::string(
            std::min<size_t>(ifd_entry.num_components, 100), '\0');
        if (IsDataInlined(ifd_entry)) {
          memcpy_s(&time_zone_offset[0], time_zone_offset.size(),
                   reinterpret_cast<const char*>(&ifd_entry.payload.data),
                   static_cast<int>(time_zone_offset.size()));
        } else {
          Expect(file.seekg(ifd_entry.payload.offset + num_header_bytes,
                            std::ios_base::beg)
                     .good(),
                 "Invalid OffsetTimeOriginal offset.");
          ReadBytes(file, &time_zone_offset[0],
                    static_cast<int>(time_zone_offset.size()));
        }
      } break;
      case kTimeZoneOffset: {
        Expect(false, "Don't know how to handle TimeZoneOffset yet.");
        break;
      }
    }
  }
  return ParseTime(date_string, time_zone_offset);
}

std::string ComposeFilename(const boost::filesystem::directory_entry& entry,
    const boost::local_time::local_date_time& time) {
  std::string parent_directory = entry.path().parent_path().filename().string();
  // Expect parent directory to be of the format:
  // 2024-12-12 Descriptive Name
  // And keep only the name
  parent_directory = parent_directory.substr(parent_directory.find(" ") + 1);
  std::stringstream out;
  out << FormatTime(time) << " " << parent_directory << " ";
  if (entry.path().filename().string().starts_with(out.str())) {
    // The output path already includes our prefix. Skip this file.
    return "";
  }
  out << entry.path().filename().stem().string() << ".jpeg";
  return out.str();
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
        // TODO: support HEIC: D:\Photos\2025\2025-01-18 Flumserberg mit Ava und Yvonne
        // TODO: support MP4: D:\Photos\2024\2024-12-06 Samichlaus
        std::osyncstream(std::cout) << "Reading: " << entry << std::endl;
        const boost::local_time::local_date_time exif_time = ReadExifData(
            entry.path().string());
        const std::string new_filename = ComposeFilename(entry, exif_time);
        if (new_filename.empty()) {
          std::osyncstream(std::cout) << "Skipping:\n   " << entry << "\n   "
              << "aleady has desired format" << std::endl;
        } else {
          std::osyncstream(std::cout) << "Renaming:\n   " << entry << "\n-> "
                                      << output_dir / new_filename << std::endl;
          boost::filesystem::rename(entry.path(), output_dir / new_filename);
        }
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
