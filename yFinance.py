import yfinance as yf
import pandas as pd

class FinancialStatementannualreport:
    def __init__(self, ticker):
        self.ticker = ticker
        self.stock = yf.Ticker(ticker)

    def fetch_income_statement(self):
        income_statement = self.stock.financials.T
        return pd.DataFrame(income_statement)

    def fetch_balance_sheet(self):
        balance_sheet = self.stock.balancesheet.T
        return pd.DataFrame(balance_sheet)

    def save_to_csv(self, statement_type):
        if statement_type == 'income_statement':
            df = self.fetch_income_statement()
            filename = f"{self.ticker}_income_statement.csv"
        elif statement_type == 'balance_sheet':
            df = self.fetch_balance_sheet()
            filename = f"{self.ticker}_balance_sheet.csv"
        else:
            print("Invalid statement type.")
            return

        # Print the DataFrame
        print(df)

        df.to_csv(filename, index=False)
        print(f"{statement_type.replace('_', ' ').title()} for {self.ticker} has been saved to {filename}.")

ticker = "LIND" 
annualreport = FinancialStatementannualreport(ticker)

annualreport.save_to_csv('income_statement')
annualreport.save_to_csv('balance_sheet')
