from dateutil import parser
import gspread
from gspread.utils import column_letter_to_index


def update_sum_formulas_in_row(fb_sheet, date_row):
    def create_sum_formula(start_col, date_row, step, total_cols):
        # Generate the range of columns for the sum formula
        formula_parts = [f'INDIRECT("{column_index_to_string( idx)}"&{date_row})' for idx in
                         range(column_letter_to_index(start_col), total_cols + 1, step)]
        return f"=SUM({','.join(formula_parts)})"

    # Starting column and step size
    start_col_letter = 'H'  # Starting from column G
    step = 7  # Step size
    total_cols = fb_sheet.col_count  # Total number of columns in the sheet

    # Iterate through the cells B, C, D
    for i in range(2, 6):  # B(2), C(3), D(4)
        # Calculate the start column letter for this iteration
        calculated_col_letter = column_index_to_string(
            column_letter_to_index(start_col_letter) + (i - 2))
        sum_formula = create_sum_formula(
            calculated_col_letter, date_row, step, total_cols)
        # Convert 2 to B, 3 to C, and 4 to D
        cell_to_update = f'{chr(64 + i)}{date_row}'
        fb_sheet.update(cell_to_update, sum_formula,
                        value_input_option='USER_ENTERED')
        # print(f'Updated cell {cell_to_update} with formula: {sum_formula}')

    ad_spend_total = gspread.utils.rowcol_to_a1(date_row, 2)
    leads_total = gspread.utils.rowcol_to_a1(date_row, 4)
    comments_total = gspread.utils.rowcol_to_a1(date_row, 5)
    for i in range(6, 8):
        calculated_col_letter = column_index_to_string(i)
        # Convert 5 to E, etc
        cell_to_update = f'{calculated_col_letter}{date_row}'
        sum_formula = ''
        if i == 6:
            sum_formula = f"=IF({leads_total}<>0,{ad_spend_total}/{leads_total},0)"
        if i == 7:
            sum_formula = f"=IF({comments_total}<>0,{ad_spend_total}/{comments_total},0)"
        fb_sheet.update(cell_to_update, sum_formula,
                        value_input_option='USER_ENTERED')
        print(
            f'percentage i={i} Updated cell {cell_to_update} with formula: {sum_formula}')


def column_index_to_string(col_index):
    """Convert a column index into a column letter: 1 -> A, 2 -> B, etc."""
    if col_index < 1:
        raise ValueError("Index is too small")
    result = ""
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def pauseMe(x=0):
    print(f" {x} - Press Enter to continue... \n")
    input()
# Function to extract the simplified AdSetName


def simplify_adset_name(name):
    parts = name.split(' | ')
    return ' | '.join(parts[-2:])

# Function to parse a date string into a datetime object


def parse_date(date_string):
    try:
        # Parse the date string into a datetime object
        return parser.parse(date_string)
    except ValueError:
        # Handle the error if the date string is in an unrecognized format
        # print(f"Error parsing date: {date_string}")
        return None
