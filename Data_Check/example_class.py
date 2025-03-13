import pandas as pd
import operator

class DataComparer:
    def __init__(self, file_path):
        self.df = pd.read_csv(file_path)
    
    def set_comparison(self, col1, col2, comparison_type='>', threshold=None):
        """
        Sets up the comparison between two columns.
        
        :param col1: The name of the first column.
        :param col2: The name of the second column.
        :param comparison_type: The type of comparison ('>', '<', '>=', '<=', '==', etc.).
        :param threshold: Optional threshold for comparison, if needed.
        """
        self.col1 = col1
        self.col2 = col2
        self.comparison_type = comparison_type
        self.threshold = threshold

        # A dictionary mapping comparison symbols to operator functions
        self.comparison_ops = {
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne
        }

        # Check if the comparison type is valid
        if self.comparison_type not in self.comparison_ops:
            raise ValueError(f"Invalid comparison type: {self.comparison_type}")

    def compare(self):
        """
        Compares the two columns using the specified comparison operator.
        
        :return: A pandas Series of boolean values where the comparison is True or False.
        """
        if self.threshold is not None:
            # Apply the comparison with the threshold (e.g., checking if col1 is greater than the threshold)
            return self.comparison_ops[self.comparison_type](self.df[self.col1], self.threshold)
        else:
            # Apply the comparison directly between the two columns
            return self.comparison_ops[self.comparison_type](self.df[self.col1], self.df[self.col2])

    def get_comparison_result(self):
        """
        Returns the comparison result as a DataFrame with a new column indicating the result.
        
        :return: DataFrame with the comparison result.
        """
        self.df['Comparison_Result'] = self.compare()
        return self.df

# Example usage:
# Assuming you have a CSV file 'data.csv' with columns 'temperature', 'humidity', 'precipitation'

# Initialize the comparer with your CSV file
comparer = DataComparer('data.csv')

# Set the comparison (e.g., temperature > humidity)
comparer.set_comparison('temperature', 'relative_humidity', '>')

# Get the comparison result (True/False for each row)
result_df = comparer.get_comparison_result()

# Show the result
print(result_df)
