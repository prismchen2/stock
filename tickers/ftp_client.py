from ftplib import FTP

ftp_address = 'ftp.nasdaqtrader.com'
dir_path = 'symboldirectory'
nasdaq_listed = 'nasdaqlisted.txt'
# others_listed = 'otherlisted.txt'


class FTPClient:
    def __init__(self):
        # set up ftp
        self.ftp = FTP(ftp_address)
        self.ftp.login()
        self.ftp.cwd(dirname=dir_path)

        self.titles = None
        self.nasdaq_tickers = []
        self.download_file(nasdaq_listed)

    def get_tickers(self):
        """
        Get all tickers from nasdaqlisted.txt

        :return: [[...]]
        """
        return self.nasdaq_tickers

    def get_titles(self):
        """
        Get title line from nasdaqlisted.txt

        :return: [str]
        """
        return self.titles

    def download_file(self, file_name):
        self.ftp.retrlines('RETR %s' % file_name, self.process_line)

    def process_line(self, line):
        line = line.split('|')
        if not self.titles:
            self.titles = line
        self.nasdaq_tickers.append(line)
