#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <ctime>
#include <limits>
#include <stdexcept>
#include <regex> // Diperlukan untuk validasi keamanan
#include <map>   // Diperlukan untuk update/select interaktif
#include <C:/Program Files/MySQL/mysql-connector-c++-8.0.33-winx64/include/jdbc/mysql_driver.h>
#include <C:/Program Files/MySQL/mysql-connector-c++-8.0.33-winx64/include/jdbc/mysql_connection.h>
#include <C:/Program Files/MySQL/mysql-connector-c++-8.0.33-winx64/include/jdbc/cppconn/statement.h>
#include <C:/Program Files/MySQL/mysql-connector-c++-8.0.33-winx64/include/jdbc/cppconn/resultset.h>
#include <C:/Program Files/MySQL/mysql-connector-c++-8.0.33-winx64/include/jdbc/cppconn/prepared_statement.h>
#include <C:/Program Files/MySQL/mysql-connector-c++-8.0.33-winx64/include/jdbc/cppconn/exception.h>

using namespace std;

/**
 * @class DatabaseManager
 * Mengelola semua koneksi dan operasi ke database MySQL.
 * Ditingkatkan dengan validasi identifier untuk keamanan
 * dan operasi data interaktif yang aman.
 */
class DatabaseManager {
private:
    sql::mysql::MySQL_Driver* driver;
    unique_ptr<sql::Connection> conn;
    string currentDB;
    mutex dbMutex;
    mutex logMutex;
    ofstream logFile;

    /**
     * @brief Menulis pesan log ke file dengan timestamp. Thread-safe.
     */
    void writeLog(const string& msg) {
        lock_guard<mutex> logLock(logMutex);
        if (!logFile.is_open()) return;

        auto now = chrono::system_clock::now();
        time_t t = chrono::system_clock::to_time_t(now);
        tm tmBuf;
#if defined(_WIN32) || defined(_WIN64)
        localtime_s(&tmBuf, &t);
#else
        localtime_r(&t, &tmBuf);
#endif
        ostringstream oss;
        oss << put_time(&tmBuf, "%Y-%m-%d %H:%M:%S");
        logFile << "[" << oss.str() << "] " << msg << endl;
    }

    /**
     * @brief [BARU] Memvalidasi string untuk keamanan identifier SQL.
     * Mencegah SQL injection pada nama tabel/database.
     */
    bool isValidIdentifier(const string& name) {
        if (name.empty() || name.length() > 64) {
            cerr << "Error: Nama identifier tidak boleh kosong atau lebih dari 64 karakter." << endl;
            return false;
        }
        // Hanya izinkan alfanumerik dan underscore. Tidak boleh dimulai dengan angka.
        // Ini adalah perlindungan kritis terhadap SQL injection pada identifier.
        static const regex idRegex("^[a-zA-Z_][a-zA-Z0-9_]*$");
        if (!regex_match(name, idRegex)) {
            cerr << "Error: Nama '" << name << "' mengandung karakter tidak valid. Hanya [a-zA-Z0-9_] yang diizinkan dan harus diawali dengan huruf atau _." << endl;
            return false;
        }
        return true;
    }

    /**
     * @brief Mendapatkan daftar kolom dan tipenya untuk sebuah tabel.
     * Digunakan oleh fungsi interaktif (insert, update, select).
     */
    map<string, string> getTableColumns(const string& tableName) {
        // Asumsi: tableName sudah divalidasi oleh pemanggil
        // Asumsi: dbMutex sudah di-lock oleh pemanggil
        map<string, string> columns;
        try {
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            unique_ptr<sql::ResultSet> res(stmt->executeQuery("DESCRIBE `" + tableName + "`"));
            while (res->next()) {
                columns[res->getString("Field")] = res->getString("Type");
            }
        } catch (sql::SQLException& e) {
            writeLog("Error getTableColumns: " + string(e.what()));
            // Biarkan map kosong, pemanggil akan menanganinya
        }
        return columns;
    }


    /**
     * @brief Memeriksa apakah database ada (versi unlocked).
     * Pemanggil harus memegang dbMutex.
     */
    bool databaseExistsUnlocked(const string& name) {
        // Asumsi: 'name' sudah divalidasi oleh pemanggil jika input dari user
        try {
            unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?"
            ));
            pstmt->setString(1, name);
            unique_ptr<sql::ResultSet> res(pstmt->executeQuery());
            return res->next();
        } catch (sql::SQLException& e) {
            writeLog(string("Error checking database existence: ") + e.what());
            return false;
        }
    }

    /**
     * @brief Mem-parsing satu baris CSV, menangani tanda kutip.
     */
    vector<string> parseCSVLine(const string& line) {
        vector<string> fields;
        stringstream ss(line);
        string field;
        bool inQuotes = false;
        char c;
        while (ss.get(c)) {
            if (inQuotes) {
                if (c == '"') {
                    if (ss.peek() == '"') {
                        field += '"';
                        ss.get();
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field += c;
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.push_back(field);
                    field.clear();
                } else if (c != '\r') {
                    field += c;
                }
            }
        }
        fields.push_back(field);
        return fields;
    }


public:
    DatabaseManager(const string& host, const string& user, const string& pass) : driver(nullptr) {
        try {
            driver = sql::mysql::get_mysql_driver_instance();
            conn.reset(driver->connect(host, user, pass));
            conn->setAutoCommit(true);
        } catch (sql::SQLException& e) {
            cerr << "Koneksi ke MySQL gagal: " << e.what() << endl;
            throw runtime_error(string("Koneksi ke MySQL gagal: ") + e.what());
        }
        logFile.open("db_operations.log", ios::app);
        if (!logFile.is_open()) {
            cerr << "Gagal membuka file log db_operations.log" << endl;
        }
        writeLog("DatabaseManager diinisialisasi.");
    }

    ~DatabaseManager() {
        writeLog("DatabaseManager ditutup.");
        if (logFile.is_open()) logFile.close();
    }

    string getCurrentDB() const {
        return currentDB;
    }

    // --- OPERASI DATABASE ---

    bool createDatabase(const string& name) {
        if (!isValidIdentifier(name)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        try {
            if (!conn) {
                cerr << "Belum terhubung ke server." << endl;
                return false;
            }
            if (databaseExistsUnlocked(name)) {
                cout << "Database '" << name << "' sudah ada. Tidak dapat membuat duplikat." << endl;
                writeLog("Gagal membuat database (duplikat): " + name);
                return false;
            }
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            // Aman karena 'name' sudah divalidasi
            stmt->execute("CREATE DATABASE `" + name + "`");
            cout << "Database '" << name << "' berhasil dibuat." << endl;
            writeLog("Membuat database: " + name);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error membuat database: " << e.what() << endl;
            writeLog(string("Error membuat database: ") + e.what());
            return false;
        }
    }

    bool listDatabases() {
        try {
            unique_ptr<sql::Statement> stmt;
            unique_ptr<sql::ResultSet> res;
            {
                lock_guard<mutex> lock(dbMutex);
                if (!conn) {
                    cerr << "Belum terhubung ke server." << endl;
                    return false;
                }
                stmt.reset(conn->createStatement());
                res.reset(stmt->executeQuery("SHOW DATABASES"));
            }
            cout << "\nDaftar Database:\n";
            cout << string(20, '-') << "\n";
            bool found = false;
            while (res->next()) {
                cout << " - " << res->getString(1) << "\n";
                found = true;
            }
            if (!found) cout << "(tidak ada database)\n";
            cout << string(20, '-') << "\n";
            writeLog("Mendaftar database.");
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Gagal menampilkan daftar database. (" << e.what() << ")\n";
            writeLog(string("Gagal menampilkan daftar database: ") + e.what());
            return false;
        }
    }

    bool dropDatabase(const string& name) {
        if (!isValidIdentifier(name)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        try {
            if (!conn) {
                cerr << "Belum terhubung ke server." << endl;
                return false;
            }
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            // Aman karena 'name' sudah divalidasi
            stmt->execute("DROP DATABASE IF EXISTS `" + name + "`");
            cout << "Database '" << name << "' dihapus (jika ada)." << endl;
            writeLog("Menghapus database: " + name);
            if (currentDB == name) currentDB.clear();
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error menghapus database: " << e.what() << endl;
            writeLog(string("Error menghapus database: ") + e.what());
            return false;
        }
    }

    bool useDatabase(const string& name) {
        if (!isValidIdentifier(name)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        try {
            if (!conn) {
                cerr << "Belum terhubung ke server." << endl;
                return false;
            }
            // 'databaseExistsUnlocked' menggunakan prepared statement, jadi aman
            if (!databaseExistsUnlocked(name)) {
                cout << "Database '" << name << "' tidak ditemukan." << endl;
                return false;
            }
            conn->setSchema(name);
            currentDB = name;
            cout << "Berhasil menggunakan database: " << name << endl;
            writeLog("Menggunakan database: " + name);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error menggunakan database: " << e.what() << endl;
            writeLog(string("Error menggunakan database: ") + e.what());
            return false;
        }
    }

    // --- OPERASI TABEL ---

    bool createTable(const string& tableName, const string& columnsDef) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan
        if (columnsDef.empty()) {
            cout << "Definisi kolom kosong." << endl;
            return false;
        }
        
        // Peringatan: columnsDef masih rentan, tapi sulit divalidasi
        // Ini adalah risiko yang harus diterima pengguna fitur ini.
        if (columnsDef.find_first_of(";'\"") != string::npos) {
             cout << "Peringatan: Definisi kolom mungkin mengandung karakter tidak aman. Lanjutkan? (y/n): ";
             char c;
             cin >> c;
             cin.ignore(numeric_limits<streamsize>::max(), '\n');
             if (c != 'y' && c != 'Y') {
                cout << "Pembuatan tabel dibatalkan." << endl;
                return false;
             }
        }


        lock_guard<mutex> lock(dbMutex);
        try {
            if (currentDB.empty()) {
                cout << "Pilih database terlebih dahulu!" << endl;
                return false;
            }
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            // Aman untuk tableName, tapi columnsDef masih berisiko
            string query = "CREATE TABLE `" + tableName + "` (" + columnsDef + ")";
            stmt->execute(query);
            cout << "Tabel '" << tableName << "' berhasil dibuat." << endl;
            writeLog("Membuat tabel: " + tableName + " di " + currentDB);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error membuat tabel: " << e.what() << endl;
            writeLog(string("Error membuat tabel: ") + e.what());
            return false;
        }
    }

    bool listTables() {
        unique_ptr<sql::ResultSet> res;
        {
            lock_guard<mutex> lock(dbMutex);
            try {
                if (currentDB.empty()) {
                    cout << "Pilih database terlebih dahulu!" << endl;
                    return false;
                }
                unique_ptr<sql::Statement> stmt(conn->createStatement());
                res.reset(stmt->executeQuery("SHOW TABLES"));
            } catch (sql::SQLException& e) {
                cerr << "Error mendaftar tabel: " << e.what() << endl;
                writeLog(string("Error mendaftar tabel: ") + e.what());
                return false;
            }
        }

        cout << "\nDaftar tabel di " << currentDB << ":" << endl;
        cout << string(20, '-') << "\n";
        bool any = false;
        while (res->next()) {
            cout << " - " << res->getString(1) << endl;
            any = true;
        }
        if (!any) cout << "(tidak ada tabel)\n";
        cout << string(20, '-') << "\n";
        writeLog("Mendaftar tabel di " + currentDB);
        return true;
    }

    bool dropTable(const string& name) {
        if (!isValidIdentifier(name)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        try {
            if (currentDB.empty()) {
                cout << "Pilih database terlebih dahulu!" << endl;
                return false;
            }
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            // Aman karena 'name' sudah divalidasi
            stmt->execute("DROP TABLE IF EXISTS `" + name + "`");
            cout << "Tabel '" << name << "' dihapus." << endl;
            writeLog("Menghapus tabel: " + name + " di " + currentDB);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error menghapus tabel: " << e.what() << endl;
            writeLog(string("Error menghapus tabel: ") + e.what());
            return false;
        }
    }

    bool describeTable(const string& tableName) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan

        unique_ptr<sql::ResultSet> res;
        {
            lock_guard<mutex> lock(dbMutex);
            try {
                if (currentDB.empty()) {
                    cout << "Pilih database terlebih dahulu!" << endl;
                    return false;
                }
                unique_ptr<sql::Statement> stmt(conn->createStatement());
                // Aman karena 'tableName' sudah divalidasi
                res.reset(stmt->executeQuery("DESCRIBE `" + tableName + "`"));
            } catch (sql::SQLException& e) {
                cerr << "Error mendeskripsikan tabel: " << e.what() << endl;
                writeLog(string("Error mendeskripsikan tabel: ") + e.what());
                return false;
            }
        }

        cout << "\nSkema tabel '" << tableName << "':" << endl;
        cout << left << setw(20) << "Kolom" << setw(20) << "Tipe" << setw(10) << "Null" << setw(10) << "Kunci" << "Default" << "Extra" << endl;
        cout << string(80, '-') << endl;
        while (res->next()) {
            cout << left << setw(20) << res->getString("Field")
                 << setw(20) << res->getString("Type")
                 << setw(10) << res->getString("Null")
                 << setw(10) << res->getString("Key")
                 << setw(10) << (res->isNull("Default") ? "NULL" : res->getString("Default"))
                 << res->getString("Extra") << endl;
        }
        writeLog("Mendeskripsikan tabel: " + tableName);
        return true;
    }

    bool truncateTable(const string& tableName) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        try {
            if (currentDB.empty()) {
                cout << "Pilih database terlebih dahulu!" << endl;
                return false;
            }
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            // Aman karena 'tableName' sudah divalidasi
            stmt->execute("TRUNCATE TABLE `" + tableName + "`");
            cout << "Tabel '" << tableName << "' dipotong." << endl;
            writeLog("Memotong tabel: " + tableName);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error memotong tabel: " << e.what() << endl;
            writeLog(string("Error memotong tabel: ") + e.what());
            return false;
        }
    }

    // --- OPERASI DATA (INTERAKTIF & AMAN) ---

    bool insertData(const string& tableName) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        try {
            if (currentDB.empty()) {
                cout << "Gunakan database terlebih dahulu!\n";
                return false;
            }

            map<string, string> columnsMap = getTableColumns(tableName);
            if (columnsMap.empty()) {
                cerr << "Tidak dapat mengambil kolom untuk tabel '" << tableName << "'." << endl;
                return false;
            }

            vector<string> columns;
            vector<string> values;
            cout << "\nMasukkan nilai untuk kolom-kolom tabel '" << tableName << "':\n";
            
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            unique_ptr<sql::ResultSet> res(stmt->executeQuery("DESCRIBE `" + tableName + "`"));

            while (res->next()) {
                string colName = res->getString("Field");
                string extra = res->getString("Extra");
                if (extra.find("auto_increment") == string::npos) {
                    columns.push_back(colName);
                    cout << colName << " (" << res->getString("Type") << "): ";
                    string val;
                    if (cin.peek() == '\n') cin.ignore(numeric_limits<streamsize>::max(), '\n');
                    getline(cin, val);
                    
                    values.push_back(val);
                }
            }

            if (columns.empty()) {
                cout << "Tidak ditemukan kolom untuk dimasukkan (mungkin hanya auto_increment?).\n";
                return false;
            }

            string query = "INSERT INTO `" + tableName + "` (";
            string valuePlaceholders = ") VALUES (";
            for (size_t i = 0; i < columns.size(); ++i) {
                query += "`" + columns[i] + "`";
                valuePlaceholders += "?";
                if (i < columns.size() - 1) {
                    query += ",";
                    valuePlaceholders += ",";
                }
            }
            query += valuePlaceholders + ");";
            
            unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(query));
            for (size_t i = 0; i < values.size(); ++i) {
                if (values[i] == "NULL" || values[i] == "null") {
                    pstmt->setNull(i + 1, sql::DataType::VARCHAR); // Lebih baik setNull
                } else {
                    pstmt->setString(i + 1, values[i]);
                }
            }

            pstmt->executeUpdate();
            cout << "Data berhasil dimasukkan ke tabel '" << tableName << "'.\n";
            writeLog("Insert otomatis ke tabel: " + tableName);
            return true;
        }
        catch (sql::SQLException& e) {
            cerr << "Error saat insert otomatis: " << e.what() << endl;
            writeLog(string("Error insert otomatis: ") + e.what());
            return false;
        }
    }

    /**
     * @brief Versi non-interaktif dari insertData (digunakan oleh generateRandomData).
     * Ini sudah aman menggunakan prepared statements.
     */
    bool insertData(const string& tableName, const vector<string>& columns, const vector<string>& values) {
        // Asumsi: tableName dan columns divalidasi oleh pemanggil jika perlu
        if (columns.empty() || columns.size() != values.size()) {
            writeLog("Error insert non-interaktif: Kolom dan nilai tidak cocok.");
            return false;
        }

        lock_guard<mutex> lock(dbMutex);
        try {
            if (currentDB.empty()) {
                writeLog("Error insert non-interaktif: DB tidak dipilih.");
                return false;
            }

            string query = "INSERT INTO `" + tableName + "` (";
            string valuePlaceholders = ") VALUES (";
            for (size_t i = 0; i < columns.size(); ++i) {
                query += "`" + columns[i] + "`"; // Asumsi nama kolom aman dari internal
                valuePlaceholders += "?";
                if (i < columns.size() - 1) {
                    query += ",";
                    valuePlaceholders += ",";
                }
            }
            query += valuePlaceholders + ");";

            unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(query));
            for (size_t i = 0; i < values.size(); ++i) {
                pstmt->setString(i + 1, values[i]);
            }
            pstmt->executeUpdate();
            return true;
        }
        catch (sql::SQLException& e) {
            writeLog(string("Error insert non-interaktif (") + tableName + "): " + e.what());
            return false;
        }
    }

    /**
     * @brief [REWRITE] Menampilkan data dengan filter WHERE interaktif dan aman.
     */
    bool selectData(const string& tableName) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan

        try {
            vector<string> whereColumns;
            vector<string> whereValues;
            map<string, string> columns;

            // 1. Dapatkan kolom dan tanyakan filter
            {
                lock_guard<mutex> lock(dbMutex);
                if (currentDB.empty()) {
                    cout << "Pilih database terlebih dahulu!" << endl;
                    return false;
                }
                columns = getTableColumns(tableName);
                if (columns.empty()) {
                    cerr << "Gagal mendapatkan kolom untuk '" << tableName << "'." << endl;
                    return false;
                }
            }

            cout << "Kolom yang tersedia: ";
            for(auto const& [key, val] : columns) cout << key << " ";
            cout << "\n";

            string colName;
            while (true) {
                cout << "Filter WHERE berdasarkan kolom (atau 'selesai'): ";
                getline(cin, colName);
                if (colName == "selesai" || colName.empty()) break;

                if (columns.find(colName) == columns.end()) {
                    cout << "Kolom '" << colName << "' tidak ditemukan." << endl;
                    continue;
                }

                cout << "Nilai untuk " << colName << " = ";
                string val;
                getline(cin, val);
                whereColumns.push_back(colName);
                whereValues.push_back(val);
            }

            // 2. Build & Eksekusi Query
            unique_ptr<sql::PreparedStatement> pstmt;
            unique_ptr<sql::ResultSet> res;
            {
                lock_guard<mutex> lock(dbMutex);
                string query = "SELECT * FROM `" + tableName + "`";
                if (!whereColumns.empty()) {
                    query += " WHERE ";
                    for (size_t i = 0; i < whereColumns.size(); ++i) {
                        query += "`" + whereColumns[i] + "` = ?";
                        if (i < whereColumns.size() - 1) query += " AND ";
                    }
                }
                
                pstmt.reset(conn->prepareStatement(query));
                for (size_t i = 0; i < whereValues.size(); ++i) {
                    pstmt->setString(i + 1, whereValues[i]);
                }
                res.reset(pstmt->executeQuery());
            }

            // 3. Tampilkan Hasil (sama seperti sebelumnya)
            cout << "\nData dari '" << tableName << "':" << endl;
            sql::ResultSetMetaData* meta = res->getMetaData();
            int cols = meta->getColumnCount();
            
            int totalWidth = 0;
            vector<int> colWidths;
            for (int i = 1; i <= cols; ++i) {
                int width = max((int)meta->getColumnName(i).length(), 15);
                colWidths.push_back(width);
                cout << left << setw(width) << meta->getColumnName(i) << " | ";
                totalWidth += width + 3;
            }
            cout << endl << string(totalWidth, '-') << endl;
            while (res->next()) {
                for (int i = 1; i <= cols; ++i) {
                    string val = res->getString(i);
                    if (val.length() > colWidths[i-1]) {
                        val = val.substr(0, colWidths[i-1] - 3) + "...";
                    }
                    cout << left << setw(colWidths[i-1]) << (res->isNull(i) ? "NULL" : val) << " | ";
                }
                cout << endl;
            }
            writeLog("Memilih data dari tabel (interaktif): " + tableName);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error memilih data: " << e.what() << endl;
            writeLog(string("Error memilih data: ") + e.what());
            return false;
        }
    }

    /**
     * @brief [REWRITE] Update data dengan panduan interaktif dan aman.
     */
    bool updateData(const string& tableName) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        if (currentDB.empty()) {
            cout << "Pilih database terlebih dahulu!" << endl;
            return false;
        }

        try {
            map<string, string> columns = getTableColumns(tableName);
            if (columns.empty()) {
                cerr << "Gagal mendapatkan kolom untuk '" << tableName << "'." << endl;
                return false;
            }

            cout << "Kolom yang tersedia: ";
            for(auto const& [key, val] : columns) cout << key << " ";
            cout << "\n";

            // 1. Get SET clauses
            vector<string> setColumns;
            vector<string> setValues;
            string colName;
            while (true) {
                cout << "Masukkan nama kolom untuk di-SET (atau 'selesai'): ";
                getline(cin, colName);
                if (colName == "selesai" || colName.empty()) break;
                
                if (columns.find(colName) == columns.end()) {
                    cout << "Kolom '" << colName << "' tidak ditemukan." << endl;
                    continue;
                }

                cout << "Nilai BARU untuk '" << colName << "': ";
                string val;
                getline(cin, val);
                setColumns.push_back(colName);
                setValues.push_back(val);
            }
            if (setColumns.empty()) {
                cout << "Tidak ada kolom yang di-SET. Operasi dibatalkan." << endl;
                return false;
            }

            // 2. Get WHERE clauses
            vector<string> whereColumns;
            vector<string> whereValues;
            while (true) {
                cout << "Filter WHERE berdasarkan kolom (atau 'selesai'): ";
                getline(cin, colName);
                if (colName == "selesai" || colName.empty()) break;

                if (columns.find(colName) == columns.end()) {
                    cout << "Kolom '" << colName << "' tidak ditemukan." << endl;
                    continue;
                }
                
                cout << "Nilai WHERE untuk '" << colName << "' (=): ";
                string val;
                getline(cin, val);
                whereColumns.push_back(colName);
                whereValues.push_back(val);
            }

            // 3. Build Query
            string query = "UPDATE `" + tableName + "` SET ";
            for (size_t i = 0; i < setColumns.size(); ++i) {
                query += "`" + setColumns[i] + "` = ?";
                if (i < setColumns.size() - 1) query += ", ";
            }
            if (!whereColumns.empty()) {
                query += " WHERE ";
                for (size_t i = 0; i < whereColumns.size(); ++i) {
                    query += "`" + whereColumns[i] + "` = ?";
                    if (i < whereColumns.size() - 1) query += " AND ";
                }
            } else {
                cout << "PERINGATAN: Update tanpa WHERE akan mempengaruhi SEMUA baris!" << endl;
                cout << "Ketik 'LANJUTKAN' untuk mengkonfirmasi: ";
                string confirm;
                getline(cin, confirm);
                if (confirm != "LANJUTKAN") {
                    cout << "Update dibatalkan." << endl;
                    return false;
                }
            }

            // 4. Prepare and Execute
            unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(query));
            int paramIndex = 1;
            for (const string& val : setValues) {
                pstmt->setString(paramIndex++, val);
            }
            for (const string& val : whereValues) {
                pstmt->setString(paramIndex++, val);
            }
            
            int rowsAffected = pstmt->executeUpdate();
            cout << rowsAffected << " baris diperbarui di '" << tableName << "'." << endl;
            writeLog("Memperbarui data di tabel (interaktif): " + tableName);
            return true;

        } catch (sql::SQLException& e) {
            cerr << "Error memperbarui data: " << e.what() << endl;
            writeLog(string("Error memperbarui data: ") + e.what());
            return false;
        }
    }

    /**
     * @brief [REWRITE] Menghapus data dengan panduan interaktif dan aman.
     */
    bool deleteData(const string& tableName) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan

        lock_guard<mutex> lock(dbMutex);
        if (currentDB.empty()) {
            cout << "Pilih database terlebih dahulu!" << endl;
            return false;
        }

        try {
            map<string, string> columns = getTableColumns(tableName);
            if (columns.empty()) {
                cerr << "Gagal mendapatkan kolom untuk '" << tableName << "'." << endl;
                return false;
            }

            cout << "Kolom yang tersedia: ";
            for(auto const& [key, val] : columns) cout << key << " ";
            cout << "\n";

            // 1. Get WHERE clauses
            vector<string> whereColumns;
            vector<string> whereValues;
            string colName;
            while (true) {
                cout << "Filter WHERE berdasarkan kolom (atau 'selesai'): ";
                getline(cin, colName);
                if (colName == "selesai" || colName.empty()) break;

                if (columns.find(colName) == columns.end()) {
                    cout << "Kolom '" << colName << "' tidak ditemukan." << endl;
                    continue;
                }
                
                cout << "Nilai WHERE untuk '" << colName << "' (=): ";
                string val;
                getline(cin, val);
                whereColumns.push_back(colName);
                whereValues.push_back(val);
            }

            // 2. Build Query
            string query = "DELETE FROM `" + tableName + "`";
            if (!whereColumns.empty()) {
                query += " WHERE ";
                for (size_t i = 0; i < whereColumns.size(); ++i) {
                    query += "`" + whereColumns[i] + "` = ?";
                    if (i < whereColumns.size() - 1) query += " AND ";
                }
            } else {
                cout << "PERINGATAN: Delete tanpa WHERE akan menghapus SEMUA data!" << endl;
                cout << "Ketik 'HAPUS SEMUA' untuk mengkonfirmasi: ";
                string confirm;
                getline(cin, confirm);
                if (confirm != "HAPUS SEMUA") {
                    cout << "Delete dibatalkan." << endl;
                    return false;
                }
            }

            // 3. Prepare and Execute
            unique_ptr<sql::PreparedStatement> pstmt(conn->prepareStatement(query));
            for (size_t i = 0; i < whereValues.size(); ++i) {
                pstmt->setString(i + 1, whereValues[i]);
            }
            
            int rowsAffected = pstmt->executeUpdate();
            cout << rowsAffected << " baris dihapus dari '" << tableName << "'." << endl;
            writeLog("Menghapus data dari tabel (interaktif): " + tableName);
            return true;

        } catch (sql::SQLException& e) {
            cerr << "Error menghapus data: " << e.what() << endl;
            writeLog(string("Error menghapus data: ") + e.what());
            return false;
        }
    }


    // --- FUNGSI UTILITAS (Backup, CSV, dll.) ---

    bool backupDatabase(const string& dbName, const string& filePath) {
        if (!isValidIdentifier(dbName)) return false; // Keamanan
        if (filePath.empty()) {
            cout << "Path file backup kosong." << endl;
            return false;
        }

        vector<string> tables;
        {
            lock_guard<mutex> lock(dbMutex);
            try {
                if (!conn) {
                    cerr << "Belum terhubung ke server." << endl;
                    return false;
                }
                if (!databaseExistsUnlocked(dbName)) {
                    cout << "Database '" << dbName << "' tidak ditemukan." << endl;
                    writeLog("Gagal backup: database tidak ditemukan: " + dbName);
                    return false;
                }
                conn->setSchema(dbName);
                unique_ptr<sql::Statement> stmt(conn->createStatement());
                unique_ptr<sql::ResultSet> res(stmt->executeQuery("SHOW TABLES"));
                while (res->next()) {
                    tables.push_back(res->getString(1)); // Nama tabel dari DB aman
                }
                // Kembalikan ke DB sebelumnya jika ada
                if (!currentDB.empty() && currentDB != dbName) {
                    conn->setSchema(currentDB);
                }
            } catch (sql::SQLException& e) {
                cerr << "Error saat menyiapkan backup: " << e.what() << endl;
                writeLog(string("Error saat menyiapkan backup: ") + e.what());
                return false;
            }
        } // Lock dilepas

        ofstream backupFile(filePath);
        if (!backupFile.is_open()) {
            cout << "Gagal membuka file backup: " << filePath << endl;
            writeLog("Gagal membuka file backup: " + filePath);
            return false;
        }

        try {
            auto now = chrono::system_clock::now();
            time_t t = chrono::system_clock::to_time_t(now);
            backupFile << "-- Backup database: " << dbName << "\n";
            backupFile << "-- Tanggal: " << ctime(&t);
            
            for (const string& table : tables) {
                backupFile << "\n-- Data untuk tabel: " << table << "\n";
                unique_ptr<sql::ResultSet> tableRes;
                sql::ResultSetMetaData* meta;
                int cols = 0;
                {
                    lock_guard<mutex> lock(dbMutex);
                    conn->setSchema(dbName); // Pastikan kita menggunakan DB yang benar
                    unique_ptr<sql::Statement> stmt(conn->createStatement());
                    tableRes.reset(stmt->executeQuery("SELECT * FROM `" + table + "`"));
                    meta = tableRes->getMetaData();
                    cols = meta->getColumnCount();
                } // Lock dilepas

                // Tulis header (nama kolom)
                for (int i = 1; i <= cols; ++i) {
                    backupFile << meta->getColumnName(i);
                    if (i < cols) backupFile << ",";
                }
                backupFile << "\n";
                // Tulis data (CSV-like)
                while (tableRes->next()) {
                    for (int i = 1; i <= cols; ++i) {
                        string v;
                        if(tableRes->isNull(i)) {
                            v = "NULL";
                        } else {
                            v = tableRes->getString(i);
                            // Escape quotes dan koma
                            if (v.find(',') != string::npos || v.find('"') != string::npos || v.find('\n') != string::npos) {
                                string tmp;
                                for (char c : v) {
                                    if (c == '"') tmp += "\"\"";
                                    else tmp += c;
                                }
                                v = "\"" + tmp + "\"";
                            }
                        }
                        backupFile << v;
                        if (i < cols) backupFile << ",";
                    }
                    backupFile << "\n";
                }
            }
            
            { // Kembalikan koneksi ke DB yang sedang digunakan
                lock_guard<mutex> lock(dbMutex);
                if (!currentDB.empty()) {
                    conn->setSchema(currentDB);
                }
            }

            backupFile.close();
            cout << "Backup '" << dbName << "' disimpan ke " << filePath << "." << endl;
            writeLog("Membackup database: " + dbName + " ke " + filePath);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error membackup database: " << e.what() << endl;
            writeLog(string("Error membackup database: ") + e.what());
            if (backupFile.is_open()) backupFile.close();
            return false;
        } catch (exception& e) {
            cerr << "Error membackup database: " << e.what() << endl;
            writeLog(string("Error membackup database (std): ") + e.what());
            if (backupFile.is_open()) backupFile.close();
            return false;
        }
    }

    bool executeQuery(const string& query) {
        if (query.empty()) {
            cout << "Query kosong." << endl;
            return false;
        }
        
        // Peringatan: Ini adalah fungsi yang SANGAT BERBAHAYA
        // Peringatkan pengguna
        string upperQuery = query;
        transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);
        
        if (upperQuery.find("DROP") != string::npos || upperQuery.find("DELETE") != string::npos || upperQuery.find("TRUNCATE") != string::npos) {
             cout << "PERINGATAN: Query Anda mengandung operasi destruktif (DROP/DELETE/TRUNCATE)." << endl;
             cout << "Ketik 'YA' untuk melanjutkan: ";
             string confirm;
             getline(cin, confirm);
             if (confirm != "YA") {
                cout << "Eksekusi dibatalkan." << endl;
                return false;
             }
        }


        lock_guard<mutex> lock(dbMutex);
        try {
            if (currentDB.empty()) {
                cout << "Pilih database terlebih dahulu!" << endl;
                return false;
            }
            unique_ptr<sql::Statement> stmt(conn->createStatement());
            
            // Coba eksekusi. Jika itu SELECT, tangani hasilnya.
            if (stmt->execute(query)) {
                unique_ptr<sql::ResultSet> res(stmt->getResultSet());
                if (res) {
                    cout << "Hasil Query (SELECT):" << endl;
                    sql::ResultSetMetaData* meta = res->getMetaData();
                    int cols = meta->getColumnCount();
                    
                    int totalWidth = 0;
                    vector<int> colWidths;
                    for (int i = 1; i <= cols; ++i) {
                        int width = max((int)meta->getColumnName(i).length(), 15);
                        colWidths.push_back(width);
                        cout << left << setw(width) << meta->getColumnName(i) << " | ";
                        totalWidth += width + 3;
                    }
                    cout << endl << string(totalWidth, '-') << endl;
                    while (res->next()) {
                        for (int i = 1; i <= cols; ++i) {
                            string val = res->getString(i);
                            if (val.length() > colWidths[i-1]) {
                                val = val.substr(0, colWidths[i-1] - 3) + "...";
                            }
                            cout << left << setw(colWidths[i-1]) << (res->isNull(i) ? "NULL" : val) << " | ";
                        }
                        cout << endl;
                    }
                } else {
                     cout << "Query (non-SELECT) berhasil dieksekusi." << endl;
                }
            } else {
                cout << "Query (non-SELECT) berhasil dieksekusi. Baris terpengaruh: " << stmt->getUpdateCount() << endl;
            }
            
            writeLog("Mengeksekusi query kustom: " + query);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error mengeksekusi query: " << e.what() << endl;
            writeLog(string("Error mengeksekusi query: ") + e.what());
            return false;
        }
    }

    bool executeQueryFromFile(const string& filePath) {
        if (filePath.empty()) {
            cout << "Path file kosong." << endl;
            return false;
        }
        
        ifstream file(filePath);
        if (!file.is_open()) {
            cout << "Gagal membuka file: " << filePath << endl;
            return false;
        }

        {
            lock_guard<mutex> lock(dbMutex);
            if (currentDB.empty()) {
                cout << "Pilih database terlebih dahulu!" << endl;
                return false;
            }
        }

        string line, query;
        int queryCount = 0;
        try {
            while (getline(file, line)) {
                size_t commentPos = line.find("--");
                if (commentPos != string::npos) line = line.substr(0, commentPos);
    
                line.erase(line.begin(), find_if(line.begin(), line.end(), [](int ch) { return !isspace(ch); }));
                line.erase(find_if(line.rbegin(), line.rend(), [](int ch) { return !isspace(ch); }).base(), line.end());

                if (line.empty()) continue;
                
                query += line + " ";

                size_t pos;
                while ((pos = query.find(';')) != string::npos) {
                    string execQ = query.substr(0, pos);
                    if (!execQ.empty()) {
                        {
                            lock_guard<mutex> lock(dbMutex);
                            unique_ptr<sql::Statement> stmt(conn->createStatement());
                            stmt->execute(execQ);
                        }
                        queryCount++;
                    }
                    query.erase(0, pos + 1);
                    query.erase(query.begin(), find_if(query.begin(), query.end(), [](int ch) { return !isspace(ch); }));
                }
            }
            
            if (!query.empty()) {
                 lock_guard<mutex> lock(dbMutex);
                 unique_ptr<sql::Statement> stmt(conn->createStatement());
                 stmt->execute(query);
                 queryCount++;
            }

            file.close();
            cout << queryCount << " query dari file berhasil dieksekusi." << endl;
            writeLog("Mengeksekusi query dari file: " + filePath);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error mengeksekusi query dari file: " << e.what() << endl;
            cerr << "Query terakhir yang gagal (mungkin): " << query << endl;
            writeLog(string("Error mengeksekusi query dari file: ") + e.what());
            file.close();
            return false;
        }
    }

    bool generateRandomData(const string& tableName, int numRows) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan
        if (numRows <= 0) {
            cout << "Jumlah baris harus > 0." << endl;
            return false;
        }
        
        // Periksa apakah tabel punya 'name' dan 'age'
        {
             lock_guard<mutex> lock(dbMutex);
             map<string, string> cols = getTableColumns(tableName);
             if (cols.find("name") == cols.end() || cols.find("age") == cols.end()) {
                cerr << "Error: Tabel '" << tableName << "' harus memiliki kolom 'name' DAN 'age' untuk fitur ini." << endl;
                return false;
             }
        }


        try {
            random_device rd;
            mt19937 gen(rd());
            uniform_int_distribution<> ageDist(18, 80);
            vector<string> names = {"Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Hank", "Ivan", "Judy", "Kyle", "Liam"};
            uniform_int_distribution<> nameDist(0, (int)names.size()-1);

            cout << "Mulai menghasilkan " << numRows << " baris..." << endl;
            for (int i = 0; i < numRows; ++i) {
                string name = names[nameDist(gen)];
                string age = to_string(ageDist(gen));
                // Gunakan insertData non-interaktif yang aman
                if (!insertData(tableName, {"name","age"}, {name, age})) {
                    // Log error sudah dilakukan di dalam insertData
                    cerr << "Gagal memasukkan data acak ke " << tableName << " pada baris " << i << endl;
                    return false;
                }
            }
            cout << "Selesai menghasilkan " << numRows << " baris acak di '" << tableName << "'." << endl;
            writeLog("Menghasilkan data acak di tabel: " + tableName);
            return true;
        } catch (exception& e) {
            cerr << "Error menghasilkan data acak: " << e.what() << endl;
            writeLog(string("Error menghasilkan data acak: ") + e.what());
            return false;
        }
    }

    bool stressTest(int numThreads, int queriesPerThread) {
        if (numThreads <= 0 || queriesPerThread <= 0) {
            cout << "Parameter tes stres harus > 0." << endl;
            return false;
        }
        
        {
            lock_guard<mutex> lock(dbMutex);
            if (currentDB.empty()) {
                cout << "Pilih database terlebih dahulu!" << endl;
                return false;
            }
        }

        try {
            vector<thread> threads;
            auto startTime = chrono::high_resolution_clock::now();

            for (int i = 0; i < numThreads; ++i) {
                threads.emplace_back([this, queriesPerThread]() {
                    for (int j = 0; j < queriesPerThread; ++j) {
                        try {
                             lock_guard<mutex> lock(dbMutex); // Lock per query
                            unique_ptr<sql::Statement> stmt(conn->createStatement());
                            unique_ptr<sql::ResultSet> r(stmt->executeQuery("SELECT 1"));
                        } catch (sql::SQLException& e) {
                            writeLog(string("Error tes stres thread: ") + e.what());
                        }
                    }
                });
            }
            
            for (auto &t : threads) {
                if (t.joinable()) t.join();
            }

            auto endTime = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime).count();
            int totalQueries = numThreads * queriesPerThread;

            cout << "Tes stres selesai." << endl;
            cout << "Total Query: " << totalQueries << endl;
            cout << "Durasi: " << duration << " ms" << endl;
            if (duration > 0) {
                 cout << "QPS (Queries Per Second): " << (totalQueries * 1000.0 / duration) << endl;
            }
            writeLog("Melakukan tes stres: " + to_string(totalQueries) + " queries selesai dalam " + to_string(duration) + " ms.");
            return true;
        } catch (exception& e) {
            cerr << "Error tes stres: " << e.what() << endl;
            writeLog(string("Error tes stres: ") + e.what());
            return false;
        }
    }

    bool exportToCSV(const string& tableName, const string& filePath) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan
        if (filePath.empty()) {
            cout << "Path file kosong." << endl;
            return false;
        }

        ofstream csvFile(filePath);
        if (!csvFile.is_open()) {
            cout << "Gagal membuka file CSV: " << filePath << endl;
            return false;
        }
        try {
            unique_ptr<sql::Statement> stmt;
            unique_ptr<sql::ResultSet> res;
            {
                lock_guard<mutex> lock(dbMutex);
                if (currentDB.empty()) {
                    cout << "Pilih database terlebih dahulu!" << endl;
                    csvFile.close();
                    return false;
                }
                stmt.reset(conn->createStatement());
                // Aman karena 'tableName' sudah divalidasi
                res.reset(stmt->executeQuery("SELECT * FROM `" + tableName + "`"));
            } 
            sql::ResultSetMetaData* meta = res->getMetaData();
            int cols = meta->getColumnCount();
            for (int i = 1; i <= cols; ++i) {
                csvFile << meta->getColumnName(i);
                if (i < cols) csvFile << ",";
            }
            csvFile << "\n";
            while (res->next()) {
                for (int i = 1; i <= cols; ++i) {
                    string value;
                    if(res->isNull(i)) {
                        value = "";
                    } else {
                        value = res->getString(i);
                        // Escape quotes dan koma
                        if (value.find(',') != string::npos || value.find('"') != string::npos || value.find('\n') != string::npos) {
                            string tmp;
                            for (char c : value) {
                                if (c == '"') tmp += "\"\"";
                                else tmp += c;
                            }
                            value = "\"" + tmp + "\"";
                        }
                    }
                    csvFile << value;
                    if (i < cols) csvFile << ",";
                }
                csvFile << "\n";
            }
            csvFile.close();
            cout << "Data dari '" << tableName << "' diekspor ke " << filePath << "." << endl;
            writeLog("Mengekspor tabel: " + tableName + " ke CSV: " + filePath);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error mengekspor ke CSV: " << e.what() << endl;
            writeLog(string("Error mengekspor ke CSV: ") + e.what());
            csvFile.close();
            return false;
        }
    }

    bool importFromCSV(const string& tableName, const string& filePath) {
        if (!isValidIdentifier(tableName)) return false; // Keamanan
        if (filePath.empty()) {
            cout << "Path file kosong." << endl;
            return false;
        }

        ifstream csvFile(filePath);
        if (!csvFile.is_open()) {
            cout << "Gagal membuka file CSV: " << filePath << endl;
            return false;
        }
        string line;
        try {
            if (!getline(csvFile, line)) {
                cout << "File CSV kosong atau gagal dibaca." << endl;
                csvFile.close();
                return false;
            }
            vector<string> columns = parseCSVLine(line);
            if (columns.empty()) {
                cout << "Header CSV kosong." << endl;
                csvFile.close();
                return false;
            }

            // [Keamanan] Validasi kolom CSV terhadap kolom tabel
            map<string, string> actualColumns;
            {
                lock_guard<mutex> lock(dbMutex);
                 if (currentDB.empty()) {
                    cout << "Pilih database terlebih dahulu!" << endl;
                    csvFile.close();
                    return false;
                }
                actualColumns = getTableColumns(tableName);
                if (actualColumns.empty()) {
                     cerr << "Gagal memverifikasi kolom tabel '" << tableName << "'." << endl;
                     csvFile.close();
                     return false;
                }
            }
            
            for (const string& csvCol : columns) {
                if (!isValidIdentifier(csvCol)) { // Periksa juga header CSV
                     cerr << "Error: Header CSV '" << csvCol << "' mengandung karakter tidak valid." << endl;
                     csvFile.close();
                     return false;
                }
                if (actualColumns.find(csvCol) == actualColumns.end()) {
                    cerr << "Error: Kolom '" << csvCol << "' dari CSV tidak ditemukan di tabel '" << tableName << "'. Impor dibatalkan." << endl;
                    csvFile.close();
                    return false;
                }
            }
            // Aman untuk melanjutkan, semua kolom CSV ada di tabel

            string query = "INSERT INTO `" + tableName + "` (";
            string valuePlaceholders = ") VALUES (";
            for (size_t i = 0; i < columns.size(); ++i) {
                query += "`" + columns[i] + "`"; // Aman karena sudah divalidasi
                valuePlaceholders += "?";
                if (i < columns.size() - 1) {
                    query += ",";
                    valuePlaceholders += ",";
                }
            }
            query += valuePlaceholders + ");";

            unique_ptr<sql::PreparedStatement> pstmt;
            {
                lock_guard<mutex> lock(dbMutex);
                pstmt.reset(conn->prepareStatement(query));
            }

            int lineCount = 0;
            int successCount = 0;
            while (getline(csvFile, line)) {
                lineCount++;
                if (line.empty() || line.find_first_not_of(" \t\r\n") == string::npos) continue;
                
                vector<string> values = parseCSVLine(line);
                if (values.size() != columns.size()) {
                    cout << "Peringatan: Melewatkan baris " << lineCount << " (jumlah kolom tidak cocok: " << values.size() << " vs " << columns.size() << ")" << endl;
                    continue;
                }

                try {
                    lock_guard<mutex> lock(dbMutex);
                    for (size_t i = 0; i < values.size(); ++i) {
                        if (values[i].empty() || values[i] == "NULL") {
                            pstmt->setNull(i + 1, sql::DataType::VARCHAR);
                        } else {
                            pstmt->setString(i + 1, values[i]);
                        }
                    }
                    pstmt->executeUpdate();
                    successCount++;
                } catch (sql::SQLException& e) {
                     cout << "Error pada baris " << lineCount << ": " << e.what() << endl;
                     writeLog("Error impor CSV baris " + to_string(lineCount) + ": " + e.what());
                }
            }

            csvFile.close();
            cout << "Selesai: " << successCount << " dari " << lineCount << " baris berhasil diimpor ke '" << tableName << "'." << endl;
            writeLog("Impor CSV ke tabel: " + tableName + " dari " + filePath);
            return true;
        } catch (sql::SQLException& e) {
            cerr << "Error kritis mengimpor dari CSV: " << e.what() << endl;
            writeLog(string("Error kritis mengimpor dari CSV: ") + e.what());
            csvFile.close();
            return false;
        } catch (exception& e) {
            cerr << "Error file saat impor CSV: " << e.what() << endl;
            writeLog(string("Error file saat impor CSV: ") + e.what());
            csvFile.close();
            return false;
        }
    }
};

// --- FUNGSI UTAMA & MENU ---

void clearScreen() {
#if defined(_WIN32) || defined(_WIN64)
    system("cls");
#else
    system("clear");
#endif
}

/**
 * @brief Menampilkan Menu Utama (Level Database)
 */
void showMainMenu() {
    cout << "\n=== MySQL C++ CLI Manager (Menu Utama) ===\n";
    cout << "Status: Belum ada database dipilih\n";
    cout << "------------------------------------------\n";
    cout << "1. List Databases\n";
    cout << "2. USE Database (Masuk ke Menu Tabel)\n";
    cout << "3. Drop Database\n";
    cout << "4. Create Database\n";
    cout << "------------------------------------------\n";
    cout << "0. Keluar\n";
    cout << "Pilihan: ";
}

/**
 * @brief Menampilkan Menu Tabel (Level Operasi)
 */
void showDatabaseMenu(const string& dbName) {
    cout << "\n=== MySQL C++ CLI Manager (Menu Tabel) ===\n";
    cout << "Database Aktif: [" << dbName << "]\n";
    cout << "------------------------------------------\n";
    cout << " 1. List Tables\n";
    cout << " 2. Create Table\n";
    cout << " 3. Drop Table\n";
    cout << " 4. Describe Table\n";
    cout << " 5. Truncate Table\n";
    cout << "------------------------------------------\n";
    cout << " 6. Insert Data (Interaktif)\n";
    cout << " 7. Select Data (Interaktif & Aman)\n";
    cout << " 8. Update Data (Interaktif & Aman)\n";
    cout << " 9. Delete Data (Interaktif & Aman)\n";
    cout << "------------------------------------------\n";
    cout << "10. Generate Random Data (Tabel 'users(name, age)')\n";
    cout << "11. Export Table to CSV\n";
    cout << "12. Import Table from CSV\n";
    cout << "13. Execute Query from File\n";
    cout << "14. Backup Database (Format Sendiri)\n";
    cout << "15. Stress Test (SELECT 1)\n";
    cout << "16. Execute Custom Query (BERBAHAYA!)\n";
    cout << "------------------------------------------\n";
    cout << " 0. Kembali ke Menu Utama\n";
    cout << "Pilihan: ";
}


void cleanCin() {
    if (cin.peek() == '\n') {
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
    }
}

/**
 * @brief Logika untuk loop Menu Tabel
 */
void databaseMenu(unique_ptr<DatabaseManager>& db) {
    int choice = -1;
    string currentDBName = db->getCurrentDB();

    while (choice != 0) {
        showDatabaseMenu(currentDBName);
        if (!(cin >> choice)) {
            cout << "Input tidak valid." << endl;
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            continue;
        }
        cleanCin(); // Bersihkan newline setelah >>

        string name, name2, query, path;
        int num;

        switch (choice) {
            case 1: db->listTables(); break;
            case 2:
                cout << "Nama tabel baru: "; getline(cin, name);
                cout << "Definisi kolom (cth: id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100)): ";
                getline(cin, query);
                db->createTable(name, query);
                break;
            case 3:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel yg dihapus: "; getline(cin, name);
                db->dropTable(name);
                break;
            case 4:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel yg dideskripsikan: "; getline(cin, name);
                db->describeTable(name);
                break;
            case 5:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel yg dipotong (truncate): "; getline(cin, name);
                db->truncateTable(name);
                break;
            case 6:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel untuk insert: "; getline(cin, name);
                db->insertData(name); // Panggil versi interaktif
                break;
            case 7:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel untuk select: "; getline(cin, name);
                db->selectData(name); // Versi interaktif baru
                break;
            case 8:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel untuk update: "; getline(cin, name);
                db->updateData(name); // Versi interaktif baru
                break;
            case 9:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel untuk delete: "; getline(cin, name);
                db->deleteData(name); // Versi interaktif baru
                break;
            case 10:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel (harus punya 'name' dan 'age'): "; getline(cin, name);
                cout << "Jumlah baris: "; cin >> num; cleanCin();
                db->generateRandomData(name, num);
                break;
            case 11:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel: "; getline(cin, name);
                cout << "Path file CSV (cth: C:/temp/export.csv): "; getline(cin, path);
                db->exportToCSV(name, path);
                break;
            case 12:
                db->listTables(); // Tampilkan daftar dulu
                cout << "Nama tabel: "; getline(cin, name);
                cout << "Path file CSV (cth: C:/temp/import.csv): "; getline(cin, path);
                db->importFromCSV(name, path);
                break;
            case 13:
                cout << "Path file SQL (cth: C:/temp/queries.sql): "; getline(cin, path);
                db->executeQueryFromFile(path);
                break;
            case 14:
                // Tampilkan daftar database untuk membantu memilih backup
                db->listDatabases();
                cout << "Nama DB yg di-backup (tidak harus DB saat ini): "; getline(cin, name);
                cout << "Path file backup (cth: C:/temp/backup.txt): "; getline(cin, path);
                db->backupDatabase(name, path);
                break;
            case 15:
                cout << "Jumlah thread: "; cin >> num; cleanCin();
                cout << "Query per thread: "; cin >> choice; cleanCin(); 
                db->stressTest(num, choice);
                choice = -1; // Reset choice agar loop tidak keluar
                break;
            case 16:
                cout << "PERINGATAN: Fitur ini menjalankan query mentah dan bisa merusak database atau tidak aman." << endl;
                cout << "Masukkan query kustom (SELECT, UPDATE, INSERT, dll.): ";
                getline(cin, query);
                db->executeQuery(query); 
                break;
            case 0:
                cout << "Kembali ke Menu Utama..." << endl;
                break;
            default:
                cout << "Pilihan tidak valid." << endl;
        }
        
        if(choice != 0) {
            cout << "\nTekan Enter untuk melanjutkan...";
            cin.get();
            clearScreen();
        } else {
            clearScreen(); // Bersihkan layar saat kembali ke menu utama
        }
    }
}


/**
 * @brief Logika untuk loop Menu Utama
 */
int main() {
    string host, user, pass;
    host = "tcp://127.0.0.1:3306";
    cout << "Menggunakan host otomatis: " << host << endl;
    cout << "Masukkan User (cth: root): ";
    getline(cin, user);
    cout << "Masukkan Password: ";
    getline(cin, pass);
    
    unique_ptr<DatabaseManager> db;
    try {
        db = make_unique<DatabaseManager>(host, user, pass);
        cout << "Koneksi berhasil." << endl;
        cout << "Tekan Enter untuk melanjutkan...";
        cin.get();
        clearScreen();
    } catch (exception& e) {
        cerr << "Koneksi GAGAL: " << e.what() << endl;
        return 1;
    }

    int choice = -1;
    while (choice != 0) {
        showMainMenu();
        if (!(cin >> choice)) {
            cout << "Input tidak valid." << endl;
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            continue;
        }
        cleanCin(); // Bersihkan newline

        string name;

        switch (choice) {
            case 1: 
                db->listDatabases(); 
                break;
            case 2:
                db->listDatabases(); // Tampilkan daftar dulu
                cout << "Nama DB yg digunakan: ";
                getline(cin, name);
                if (db->useDatabase(name)) {
                    clearScreen();
                    databaseMenu(db); // Masuk ke menu tabel
                }
                break;
            case 3:
                db->listDatabases(); // Tampilkan daftar dulu
                cout << "Nama DB yg dihapus: ";
                getline(cin, name);
                db->dropDatabase(name);
                break;
            case 4:
                cout << "Nama DB baru: ";
                getline(cin, name);
                db->createDatabase(name);
                break;
            case 0:
                cout << "Keluar..." << endl;
                break;
            default:
                cout << "Pilihan tidak valid." << endl;
        }
        
        if(choice != 0 && choice != 2) { // Jangan pause jika baru keluar dari submenu
            cout << "\nTekan Enter untuk melanjutkan...";
            cin.get();
            clearScreen();
        }
    }

    return 0;
}