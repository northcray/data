import random
import time

from dagster import asset, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

def get_form(page):
    page.goto("https://pa.bexley.gov.uk/online-applications")

    try:
        page.get_by_role("link", name="Property").click()
    except PlaywrightTimeoutError:
        context.log.error("Timeout for property button")
        page.screenshot(path=f'property_button.png')

def fill_form(page, uprn, context):
    page.locator("input#uprn").fill(str(uprn))
    page.locator("[type=submit]").click()
    title = page.title()
    property_id = title.split('|')[0].strip()

    context.log.debug(f"title: {title}")

    try:
        error_message = page.locator('.messagebox').inner_text(timeout=500)
    except PlaywrightTimeoutError:
        error_message = None

    if property_id == "429 Too Many Requests":
        context.log.error("Rate limited!")
        page.screenshot(path=f'limit_429.png')
        time.sleep(60) # wait 1:05 mins
        get_form(page)
        return fill_form(page, uprn, context)  #  recursion, exciting!
    elif title == "Address Search":
        # something when wrong
        context.log.error(f"Error ({uprn}): {error_message}")
        return None
    elif len(property_id) == 13:
        return property_id
    else:
        page.screenshot(path=f'error_{uprn}.png')
        return None


@asset(
    group_name="bexley",
    deps=["uprn"]
)
def addresses(context: AssetExecutionContext, duckdb: DuckDBResource) -> Output[None]:
    with duckdb.get_connection() as conn:
        # Create the table in DuckDB if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS address_data (
                uprn BIGINT,
                property_id TEXT,
                full_address TEXT,
                property_description TEXT,
                property_number TEXT,
                street TEXT,
                town TEXT,
                postcode TEXT,
                ward TEXT,
                parish TEXT
            )
        """)

        conn.execute("CREATE TABLE IF NOT EXISTS uprn_ignore (uprn BIGINT, reason TEXT)")

        total = conn.execute("SELECT count(uprn) as count from uprns;").fetchall()[0][0]
        todo_ids = """
        SELECT DISTINCT uprns.uprn
        FROM uprns
        WHERE uprns.uprn NOT IN (
            SELECT uprn FROM address_data
            UNION
            SELECT uprn FROM uprn_ignore
        );
        """
        uprn_list = conn.execute(todo_ids).df()['uprn'].tolist()
        remaining = len(uprn_list)
        done = total - remaining

        # Fetch and store data using Playwright
        with sync_playwright() as playwright:
            # Start a new browser session
            browser = playwright.chromium.launch()
            page = browser.new_page()
            get_form(page)

            for uprn in uprn_list:
                remaining = total - done
                done += 1
                context.log.info(f"{uprn} (UPRN) - {done}/{total} {done/total:.2%} completed - {remaining} {remaining/total:.2%} remaining")

                property_id = fill_form(page, uprn, context)

                if property_id is None:
                    context.log.error(f"Something wrong with {uprn}")
                    conn.execute(f"""INSERT INTO uprn_ignore BY NAME (SELECT {uprn} AS uprn, NULL AS reason);""")
                    continue

                try:
                    # Mapping headers to snake_case column names
                    headers_map = {
                        "UPRN": "uprn",
                        "Full Address": "full_address",
                        "Property Description": "property_description",
                        "Property Number": "property_number",
                        "Street": "street",
                        "Town": "town",
                        "Postcode": "postcode",
                        "Ward": "ward",
                        "Parish": "parish"
                    }

                    # Parse table data into a dictionary
                    data = {"uprn": uprn, "property_id": property_id}

                    property_table = page.wait_for_selector("table#propertyAddress")

                    # Get all rows from the table
                    rows = property_table.query_selector_all("tr")

                    # Parse table data into a dictionary
                    for row in rows:
                        # Get header and value from each row
                        header = row.query_selector("th").text_content().strip().replace(":", "")
                        value = row.query_selector("td").text_content().strip()

                        # Convert to snake_case using the mapping
                        snake_case_header = headers_map.get(header)

                        if snake_case_header:
                            # Convert UPRN to integer
                            if snake_case_header == "uprn":
                                value = int(value)
                            data[snake_case_header] = value

                    # time.sleep(random.randint(3, 5))

                    # Go back to the search page for the next UPRN
                    page.click("#refinesearch")

                    # time.sleep(random.randint(3, 5))

                    context.log.debug(data)
                    conn.execute(
                        """
                        INSERT INTO address_data (
                            uprn, property_id, full_address, property_description, 
                            property_number, street, town, postcode, ward, parish
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            data["uprn"],
                            data["property_id"],
                            data.get("full_address", ""),
                            data.get("property_description", ""),
                            data.get("property_number", ""),
                            data.get("street", ""),
                            data.get("town", ""),
                            data.get("postcode", ""),
                            data.get("ward", ""),
                            data.get("parish", "")
                        )
                    )

                except PlaywrightTimeoutError:
                    context.log.error(f"Timeout for {uprn}")

            # Close the browser
            browser.close()

        conn.execute("copy (select * from address_data) to 'data/address_data.csv';")

    return Output(None)
