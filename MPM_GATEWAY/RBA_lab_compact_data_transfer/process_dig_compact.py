from bs4 import BeautifulSoup, Comment
import pandas as pd

def etl(soup,last_id):
    """
    Extracts structured data from HTML comments containing table rows.

    Parameters:
        html_content (str): The HTML content of the page.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted table data.
    """
    # soup = BeautifulSoup(html_content, 'html.parser')
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))

    structured_data = []

    # Loop through the comments to find the one with the data
    for comment in comments:
        if "Create a New Row" in comment:
            row = comment.find_next('tr')

            # Extract the data from the row and structure it in a dictionary
            row_data = {}
            columns = row.find_all('td')

            if len(columns) == 6:
                row_data['CycleNo'] = columns[0].text.strip()
                row_data['Datetime'] = pd.to_datetime(columns[1].text.strip() +" "+ columns[2].text.strip(),format = "%d-%m-%Y %H:%M:%S")
                row_data['Mode'] = columns[3].text.strip()
                row_data['Value'] = columns[4].text.strip()
                row_data['Unit'] = columns[5].text.strip()

            structured_data.append(row_data)
    df = pd.DataFrame(structured_data)
    if last_id:
        df = df[df["Datetime"]>pd.to_datetime(last_id)]
    df.sort_values("Datetime",inplace = True)
    return df