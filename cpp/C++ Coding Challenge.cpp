#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <thread>
#include <mutex>
#include <chrono>

// Structure to hold market data entry
struct MarketDataEntry {
    std::string timestamp;
    std::string symbol;
    double price;
    int size;
    std::string exchange;
    std::string type;
};

// Function to read market data from a file
std::vector<MarketDataEntry> ReadMarketDataFromFile(const std::string& filename) {
    std::vector<MarketDataEntry> marketData;


    std::ifstream file(filename);
    if (file.is_open()) {
    	std::string symbol = filename.substr(0, filename.find("."));
		std::string firstLine;
        std::getline(file, firstLine); 
        std::string line;
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            MarketDataEntry entry;
            std::getline(iss, entry.timestamp, ',');
            entry.symbol=symbol;
            iss >> entry.price;
            iss.ignore();
            iss >> entry.size;
            iss.ignore();
            std::getline(iss, entry.exchange, ',');
            std::getline(iss, entry.type, ',');
            marketData.push_back(entry);
        }
        file.close();
    }

    return marketData;
}

// Function to write market data to a file
void WriteMarketDataToFile(const std::string& filename, const std::vector<MarketDataEntry>& marketData) {

    std::ofstream file(filename, std::ios::app);
    if (file.is_open()) {
    	
        for (const MarketDataEntry& entry : marketData) {
            file << entry.symbol << ", " << entry.timestamp << ", " << entry.price << ", "
                 << entry.size << ", " << entry.exchange << ", " << entry.type << "\n";
        }
        file.close();
    }
}
// Function to compare market data entries based on timestamp and symbol
bool CompareMarketDataEntries(const MarketDataEntry& entry1, const MarketDataEntry& entry2) {
    if (entry1.timestamp == entry2.timestamp) {
        return entry1.symbol < entry2.symbol;
    }
    return entry1.timestamp < entry2.timestamp;
}

std::string Coverttotime(const std::string& intervalStart){
	std::string timestampStr = intervalStart;
    long long timestamp = std::stoll(timestampStr);  // Convert the string to a long long

    		// Convert the timestamp to a time_point
    std::chrono::milliseconds msTimestamp(timestamp);

    		// Convert the time_point to a time_t object
    std::time_t time = std::chrono::duration_cast<std::chrono::seconds>(msTimestamp).count();

    		// Format the time as a string
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
    //std::cout << ss.str() << std::endl;
    return ss.str();
}



// Function to process market data for a given chunk and interval
void ProcessMarketData(const std::vector<std::string>& files, const std::string& intervalStart, const std::string& intervalEnd,
                       std::mutex& mtx, std::vector<MarketDataEntry>& marketData) {
    std::string intervalStart1=Coverttotime(intervalStart);
	std::string intervalEnd1=Coverttotime(intervalEnd);

    for (const std::string& file : files) {
        std::vector<MarketDataEntry> fileData = ReadMarketDataFromFile(file);


        for (const MarketDataEntry& entry : fileData) {
            if (entry.timestamp >= intervalStart1 && entry.timestamp < intervalEnd1) {
                std::lock_guard<std::mutex> lock(mtx);
                marketData.push_back(entry);
            }
        }
    }


}

long long ConvertTimestampToMillis(const std::string& timestamp) {
    std::tm tm = {};
    std::istringstream ss(timestamp);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();

    // Extract the milliseconds from the timestamp string
    size_t dotPos = timestamp.find(".");
    if (dotPos != std::string::npos) {
        std::string millisecondsStr = timestamp.substr(dotPos + 1);
        ms += std::stoll(millisecondsStr);
    }

    return ms;
}



int main() {
    // List the file names here...
    std::vector<std::string> fileNames ;
    fileNames.push_back("CSCO.txt");
    fileNames.push_back("MSFT.txt");
    
    ///fileNames.push_back("File2.txt"); // self created dummy data 
	//fileNames.push_back("File3.txt"); // self created dummy data 
	//fileNames.push_back("File4.txt"); // self created dummy data 
	//fileNames.push_back("File5.txt"); // self created dummy data 
	//fileNames.push_back("File6.txt"); // self created dummy data 
	//fileNames.push_back("File7.txt"); // self created dummy data 
	//fileNames.push_back("File8.txt"); // self created dummy data 
	//fileNames.push_back("File9.txt"); // self created dummy data 
	//fileNames.push_back("File10.txt"); // self created dummy data 
	//fileNames.push_back("AAB.txt");   // self created dummy data 
	
	
    //push more files
    
    //std::sort(fileNames.begin(), fileNames.end());

    // Number of files to process concurrently
	const int maxThreads = std::thread::hardware_concurrency();//2 since the files are less
    int chunkSize = 100;//3 since the files are less
    std::mutex mtx;
    std::string outputFile = "MultiplexedFile.txt";
    		    
    // Divide the files into chunks
    std::vector<std::vector<std::string>> fileChunks;
    int numChunks = fileNames.size() / chunkSize;
    if (fileNames.size() % chunkSize != 0)
        numChunks++;
    for (int i = 0; i < numChunks; ++i) {
        int startIndex = i * chunkSize;
        int endIndex = (i == numChunks - 1) ? fileNames.size() : (i + 1) * chunkSize;
        std::vector<std::string> chunk(fileNames.begin() + startIndex, fileNames.begin() + endIndex);
        fileChunks.push_back(chunk);
    }

    // Process market data for each chunk and interval
    
    
    
    std::ofstream output(outputFile);
    output << "Symbol, Timestamp, Price, Size, Exchange, Type"<<"\n";
    output.close();
    // Read the start time and end time from any one file
    //std::vector<MarketDataEntry> firstFileData = ReadMarketDataFromFile(fileNames[0]);
    //std::string startTime = firstFileData.front().timestamp;
    //std::string endTime = firstFileData.back().timestamp;
        
    // Define the start and end timestamps for the intervals
    std::string startTime = "2021-03-05 10:00:00.000";
    std::string endTime = "2021-03-05 16:00:00.000";
        
    

    
    // Convert start time and end time to milliseconds
    long long startMillis = ConvertTimestampToMillis(startTime);
	long long endMillis = ConvertTimestampToMillis(endTime);
   

    // Create five-minute intervals
    std::vector<std::string> intervals;
    const long long intervalDuration = 5 * 60 * 1000; // 5 minutes in milliseconds

    for (long long intervalStart = startMillis; intervalStart < endMillis; intervalStart += intervalDuration) {
        long long intervalEnd = intervalStart + intervalDuration;
        
        
        std::string interval = std::to_string(intervalStart) + "," + std::to_string(intervalEnd);
        //std::cout<<interval<<std::endl; 
        intervals.push_back(interval);   
    	

    }




    for (const std::string& interval : intervals) {
        std::vector<MarketDataEntry> marketData;
		std::vector<std::thread> threads;

    	
		int threadsCreated = 0;

		for (int i = 0; i < numChunks; i += maxThreads) {
    		int endIndex = std::min(i + maxThreads, numChunks);
    
    		std::vector<std::thread> threads;
    		for (int j = i; j < endIndex; ++j) {
        		threads.emplace_back(ProcessMarketData, std::cref(fileChunks[j]), interval.substr(0, 13),
                             interval.substr(14), std::ref(mtx), std::ref(marketData));
    		}
    
    		for (std::thread& t : threads) {
        		t.join();
    		}
    
    		threadsCreated += endIndex - i;
    
    		// Check if all files have been processed
    		if (threadsCreated >= numChunks) {
        		break;
    		}
		}
    	
    	std::sort(marketData.begin(), marketData.end(), CompareMarketDataEntries);

    	// Write market data to intermediate file
    	WriteMarketDataToFile("MultiplexedFile.txt", marketData);
    }


    return 0;
}