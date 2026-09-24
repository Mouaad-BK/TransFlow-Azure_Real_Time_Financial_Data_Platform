import os
import io

import joblib
import pandas as pd
import numpy as np

import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle
)


app = func.FunctionApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)


@app.route(route="anomaly_detection")
def anomaly_detection(req: func.HttpRequest) -> func.HttpResponse:

    try:

        # ============================================================
        # 1. Load model
        # ============================================================

        model_path = os.path.join(
            os.path.dirname(__file__),
            "model",
            "isolation_forest.joblib"
        )

        model = joblib.load(model_path)


        # ============================================================
        # 2. Connect to ADLS
        # ============================================================

        storage_key = os.getenv("STORAGE_ACCOUNT_KEY")

        service_client = DataLakeServiceClient(
            account_url="https://strtransactions.dfs.core.windows.net",
            credential=storage_key
        )

        file_system_client = service_client.get_file_system_client(
            "transactions"
        )


        # ============================================================
        # 3. Read Silver Delta table
        # ============================================================

        silver_path = (
            "abfss://transactions@strtransactions.dfs.core.windows.net/silver"
        )

        silver_table = DeltaTable(
            silver_path,
            storage_options={
                "azure_storage_account_name": "strtransactions",
                "azure_storage_access_key": storage_key
            }
        )

        silver_df = silver_table.to_pandas()


        # ============================================================
        # 4. Read ML checkpoint
        # ============================================================

        checkpoint_path = (
            "ml_checkpoint/last_processed_timestamp.txt"
        )

        checkpoint_file = file_system_client.get_file_client(
            checkpoint_path
        )

        checkpoint_content = (
            checkpoint_file.download_file()
            .readall()
            .decode("utf-8")
            .strip()
        )


        # ============================================================
        # 5. Filter new transactions
        # ============================================================

        silver_df["timestamp"] = pd.to_datetime(
            silver_df["timestamp"],
            utc=True
        )

        if checkpoint_content:

            checkpoint_timestamp = pd.to_datetime(
                checkpoint_content,
                utc=True
            )

        else:

            checkpoint_timestamp = pd.Timestamp(
                0,
                tz="UTC"
            )

        new_transactions_df = silver_df[
            silver_df["timestamp"] > checkpoint_timestamp
        ].copy()


        # Keep original data for reporting
        report_df = new_transactions_df.copy()


        # ============================================================
        # 6. Detection time
        # ============================================================

        detection_time = pd.Timestamp.now(tz="UTC")


        # ============================================================
        # 7. Fixed risk-score bounds
        #    Obtained from validation dataset
        # ============================================================

        score_min = -0.06804481267415496
        score_max = 0.21720950732373295


        # ============================================================
        # 8. Prepare prediction data
        # ============================================================

        prediction_df = new_transactions_df.copy()


        # ------------------------------------------------------------
        # Missing values
        # ------------------------------------------------------------

        prediction_df["merchant_state"] = (
            prediction_df["merchant_state"].fillna("Unknown")
        )

        prediction_df["transaction_error"] = (
            prediction_df["transaction_error"].fillna("Success")
        )


        # ------------------------------------------------------------
        # Normalize categorical values
        # ------------------------------------------------------------

        prediction_df["card_usage_method"] = (
            prediction_df["card_usage_method"].replace({
                "Swipe Transaction": "Swipe",
                "Chip Transaction": "Chip",
                "Online Transaction": "Online"
            })
        )

        prediction_df["card_brand"] = (
            prediction_df["card_brand"].replace({
                "visa": "Visa"
            })
        )

        prediction_df["card_type"] = (
            prediction_df["card_type"].replace({
                "credit": "Credit"
            })
        )


        # ------------------------------------------------------------
        # Encode binary values
        # ------------------------------------------------------------

        prediction_df["gender"] = prediction_df["gender"].map({
            "Male": 0,
            "Female": 1
        })

        prediction_df["has_chip"] = (
            prediction_df["has_chip"].astype(int)
        )

        prediction_df["transaction_error"] = (
            prediction_df["transaction_error"]
            .apply(lambda x: 0 if x == "Success" else 1)
        )


        # ============================================================
        # 9. Standardize US state names
        # ============================================================

        state_mapping = {

            "AA": "Armed Forces Americas",
            "AK": "Alaska",
            "AL": "Alabama",
            "AR": "Arkansas",
            "AZ": "Arizona",
            "CA": "California",
            "CO": "Colorado",
            "CT": "Connecticut",
            "DC": "District of Columbia",
            "DE": "Delaware",
            "FL": "Florida",
            "GA": "Georgia",
            "HI": "Hawaii",
            "IA": "Iowa",
            "ID": "Idaho",
            "IL": "Illinois",
            "IN": "Indiana",
            "KS": "Kansas",
            "KY": "Kentucky",
            "LA": "Louisiana",
            "MA": "Massachusetts",
            "MD": "Maryland",
            "ME": "Maine",
            "MI": "Michigan",
            "MN": "Minnesota",
            "MO": "Missouri",
            "MS": "Mississippi",
            "MT": "Montana",
            "NC": "North Carolina",
            "ND": "North Dakota",
            "NE": "Nebraska",
            "NH": "New Hampshire",
            "NJ": "New Jersey",
            "NM": "New Mexico",
            "NV": "Nevada",
            "NY": "New York",
            "OH": "Ohio",
            "OK": "Oklahoma",
            "OR": "Oregon",
            "PA": "Pennsylvania",
            "RI": "Rhode Island",
            "SC": "South Carolina",
            "SD": "South Dakota",
            "TN": "Tennessee",
            "TX": "Texas",
            "UT": "Utah",
            "VA": "Virginia",
            "VT": "Vermont",
            "WA": "Washington",
            "WI": "Wisconsin",
            "WV": "West Virginia",
            "WY": "Wyoming"
        }

        prediction_df["merchant_state"] = (
            prediction_df["merchant_state"]
            .map(state_mapping)
            .fillna(prediction_df["merchant_state"])
        )


        # ============================================================
        # 10. Group locations into geographic regions
        # ============================================================

        region_mapping = {

            # North America
            **dict.fromkeys([
                "Armed Forces Americas",
                "Alaska",
                "Alabama",
                "Arkansas",
                "Arizona",
                "California",
                "Colorado",
                "Connecticut",
                "District of Columbia",
                "Delaware",
                "Florida",
                "Georgia",
                "Hawaii",
                "Idaho",
                "Illinois",
                "Indiana",
                "Iowa",
                "Kansas",
                "Kentucky",
                "Louisiana",
                "Maine",
                "Maryland",
                "Massachusetts",
                "Michigan",
                "Minnesota",
                "Mississippi",
                "Missouri",
                "Montana",
                "Nebraska",
                "Nevada",
                "New Hampshire",
                "New Jersey",
                "New Mexico",
                "New York",
                "North Carolina",
                "North Dakota",
                "Ohio",
                "Oklahoma",
                "Oregon",
                "Pennsylvania",
                "Rhode Island",
                "South Carolina",
                "South Dakota",
                "Tennessee",
                "Texas",
                "Utah",
                "Vermont",
                "Virginia",
                "Washington",
                "West Virginia",
                "Wisconsin",
                "Wyoming",
                "Canada",
                "Mexico",
                "Belize",
                "Costa Rica",
                "Dominican Republic",
                "Haiti",
                "Honduras",
                "Jamaica",
                "Aruba",
                "The Bahamas",
                "Trinidad and Tobago"
            ], "North America"),

            # South America
            **dict.fromkeys([
                "Argentina",
                "Brazil",
                "Colombia",
                "Peru",
                "Uruguay"
            ], "South America"),

            # Europe
            **dict.fromkeys([
                "Andorra",
                "Austria",
                "Auvergne-Rhône-Alpes",
                "Bavaria",
                "Belgium",
                "Berlin",
                "Bosnia and Herzegovina",
                "Brussels",
                "Campania",
                "Capital Region",
                "Catalonia",
                "Community of Madrid",
                "Czech Republic",
                "Denmark",
                "Finland",
                "France",
                "Geneva",
                "Germany",
                "Greece",
                "Hamburg",
                "Hungary",
                "Ireland",
                "Italy",
                "Lazio",
                "Leinster",
                "Lisbon",
                "Lithuania",
                "Lombardy",
                "Luxembourg",
                "Macedonia",
                "Masovian",
                "Netherlands",
                "North Holland",
                "Norway",
                "Oslo",
                "Poland",
                "Porto",
                "Portugal",
                "Prague",
                "Provence-Alpes-Côte d'Azur",
                "Romania",
                "Russia",
                "South Holland",
                "Spain",
                "Stockholm",
                "Sweden",
                "Switzerland",
                "Uusimaa",
                "United Kingdom",
                "Valencian Community",
                "Vatican City",
                "Vienna",
                "Zurich",
                "Île-de-France"
            ], "Europe"),

            # Asia
            **dict.fromkeys([
                "China",
                "Hong Kong",
                "India",
                "Indonesia",
                "Japan",
                "Malaysia",
                "Mongolia",
                "Pakistan",
                "Philippines",
                "Singapore",
                "South Korea",
                "Sri Lanka",
                "Taiwan",
                "Thailand"
            ], "Asia"),

            # Middle East
            **dict.fromkeys([
                "Iran",
                "Saudi Arabia",
                "Turkey",
                "United Arab Emirates"
            ], "Middle East"),

            # Africa
            **dict.fromkeys([
                "Burkina Faso",
                "Cabo Verde",
                "Cote d'Ivoire",
                "Egypt",
                "Equatorial Guinea",
                "Kenya",
                "Nigeria",
                "South Africa",
                "South Sudan"
            ], "Africa"),

            # Oceania
            **dict.fromkeys([
                "Australia",
                "New Zealand",
                "Papua New Guinea"
            ], "Oceania")
        }

        prediction_df["merchant_region"] = (
            prediction_df["merchant_state"]
            .map(region_mapping)
            .fillna("Unknown")
        )


        # ============================================================
        # 11. Group MCC into merchant category families
        # ============================================================

        def map_mcc_family(mcc):

            mcc = int(mcc)

            if 3000 <= mcc < 4000:
                return "Travel_Transport"

            elif 4000 <= mcc < 5000:
                return "Transport_Utilities"

            elif 5000 <= mcc < 6000:
                return "Retail"

            elif 6000 <= mcc < 7000:
                return "Financial"

            elif 7000 <= mcc < 8000:
                return "Services_Leisure"

            elif 8000 <= mcc < 9000:
                return "Professional_Medical"

            elif 9000 <= mcc < 10000:
                return "Government"

            else:
                return "Other"


        prediction_df["merchant_category_code"] = (
            prediction_df["merchant_category_code"]
            .apply(map_mcc_family)
        )


        # ============================================================
        # 12. Apply signed log transformation to amount
        # ============================================================

        prediction_df["amount"] = (
            prediction_df["amount"].astype(float)
        )

        prediction_df["amount"] = (
            np.sign(prediction_df["amount"])
            * np.log1p(np.abs(prediction_df["amount"]))
        )


        # ============================================================
        # 13. Remove annual income values excluded during training
        # ============================================================

        prediction_df = prediction_df[
            prediction_df["annual_income"] != 1
        ].copy()


        # ============================================================
        # 14. Ensure card_limit matches model feature name
        # ============================================================

        if (
            "card_limit" in prediction_df.columns
            and "credit_limit" not in prediction_df.columns
        ):
            prediction_df["credit_limit"] = (
                prediction_df["card_limit"]
            )


        # ============================================================
        # 15. Remove columns not used by the model
        # ============================================================

        prediction_df = prediction_df.drop(
            columns=[
                "transaction_id",
                "customer_id",
                "card_id",
                "merchant_id",
                "timestamp",
                "postal_code",
                "merchant_city",
                "merchant_state",
                "card_limit"
            ],
            errors="ignore"
        )


        # ============================================================
        # 16. One-hot encode categorical features
        # ============================================================

        categorical_features = [
            "card_usage_method",
            "card_brand",
            "card_type",
            "merchant_region",
            "merchant_category_code"
        ]

        prediction_df = pd.get_dummies(
            prediction_df,
            columns=categorical_features,
            dtype=int
        )


        # ============================================================
        # 17. Keep exactly the features used during training
        # ============================================================

        model_features = [

            "amount",
            "transaction_error",
            "age",
            "gender",
            "annual_income",
            "credit_score",
            "number_of_cards",
            "has_chip",
            "credit_limit",

            "card_usage_method_Chip",
            "card_usage_method_Contactless",
            "card_usage_method_Online",
            "card_usage_method_Swipe",

            "card_brand_Amex",
            "card_brand_Discover",
            "card_brand_Mastercard",
            "card_brand_Visa",

            "card_type_Credit",
            "card_type_Debit",
            "card_type_Debit (Prepaid)",

            "merchant_region_Africa",
            "merchant_region_Asia",
            "merchant_region_Europe",
            "merchant_region_Middle East",
            "merchant_region_North America",
            "merchant_region_Oceania",
            "merchant_region_South America",
            "merchant_region_Unknown",

            "merchant_category_code_Financial",
            "merchant_category_code_Government",
            "merchant_category_code_Other",
            "merchant_category_code_Professional_Medical",
            "merchant_category_code_Retail",
            "merchant_category_code_Services_Leisure",
            "merchant_category_code_Transport_Utilities",
            "merchant_category_code_Travel_Transport"
        ]


        prediction_df = prediction_df.reindex(
            columns=model_features,
            fill_value=0
        )


        # ============================================================
        # 18. Prediction
        # ============================================================

        if len(prediction_df) == 0:

            # No new transactions
            high_risk_df = pd.DataFrame(
                columns=[
                    "transaction_id",
                    "customer_id",
                    "amount",
                    "anomaly_score",
                    "transaction_date"
                ]
            )

        else:

            # --------------------------------------------------------
            # Predict anomalies
            # --------------------------------------------------------

            predictions = model.predict(
                prediction_df
            )

            scores = model.decision_function(
                prediction_df
            )


            # --------------------------------------------------------
            # Calculate fixed anomaly risk score
            # --------------------------------------------------------

            risk_scores = (
                (score_max - scores)
                / (score_max - score_min)
            ) * 100


            risk_scores_series = pd.Series(
                risk_scores,
                index=prediction_df.index
            )


            # --------------------------------------------------------
            # Select high-risk transactions
            # --------------------------------------------------------

            high_risk_indices = risk_scores_series[
                risk_scores_series >= 80
            ].index


            high_risk_df = report_df.loc[
                high_risk_indices,
                [
                    "transaction_id",
                    "customer_id",
                    "amount",
                    "timestamp"
                ]
            ].copy()


            # Keep anomaly score numeric
            high_risk_df["anomaly_score"] = (
                risk_scores_series
                .loc[high_risk_indices]
                .round(2)
            )


            # Rename timestamp
            high_risk_df = high_risk_df.rename(
                columns={
                    "timestamp": "transaction_date"
                }
            )


            # Final column order
            high_risk_df = high_risk_df[
                [
                    "transaction_id",
                    "customer_id",
                    "amount",
                    "anomaly_score",
                    "transaction_date"
                ]
            ]


        # ============================================================
        # 19. Generate PDF report
        # ============================================================

        report_time = detection_time.floor("h")

        report_filename = (
            f"{report_time.strftime('%Y-%m-%d_%H')}:00.pdf"
        )


        buffer = io.BytesIO()


        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            rightMargin=15 * mm,
            leftMargin=15 * mm,
            topMargin=18 * mm,
            bottomMargin=18 * mm
        )


        styles = getSampleStyleSheet()


        title_style = ParagraphStyle(
            "Title",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=20,
            leading=24,
            alignment=TA_CENTER,
            spaceAfter=4
        )


        subtitle_style = ParagraphStyle(
            "Subtitle",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            alignment=TA_CENTER,
            spaceAfter=18
        )


        report_title_style = ParagraphStyle(
            "ReportTitle",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=15,
            leading=19,
            alignment=TA_CENTER,
            spaceAfter=6
        )


        period_style = ParagraphStyle(
            "Period",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=9,
            leading=12,
            alignment=TA_CENTER,
            spaceAfter=20
        )


        section_style = ParagraphStyle(
            "Section",
            parent=styles["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=14,
            alignment=TA_CENTER,
            spaceAfter=5
        )


        text_style = ParagraphStyle(
            "Text",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=9,
            leading=13,
            alignment=TA_CENTER,
            spaceAfter=15
        )


        story = []


        # ------------------------------------------------------------
        # Header
        # ------------------------------------------------------------

        story.append(
            Paragraph(
                "TRANSFLOW PLATFORM",
                title_style
            )
        )


        story.append(
            Paragraph(
                "Real-Time Payment Processing",
                subtitle_style
            )
        )


        story.append(
            Paragraph(
                "HOURLY ANOMALY DETECTION REPORT",
                report_title_style
            )
        )


        story.append(
            Paragraph(
                f"Detection period: "
                f"{report_time.strftime('%d %B %Y — %H:%M UTC')}",
                period_style
            )
        )


        story.append(
            Paragraph(
                "HIGH-RISK TRANSACTIONS",
                section_style
            )
        )


        # ============================================================
        # 20. PDF content depending on transaction availability
        # ============================================================

        if len(new_transactions_df) == 0:

            story.append(
                Paragraph(
                    "No transaction occurred during the last hour.",
                    text_style
                )
            )

            story.append(
                Paragraph(
                    "No high-risk transaction with anomaly score ≥ 80%.",
                    text_style
                )
            )


        elif len(high_risk_df) == 0:

            story.append(
                Paragraph(
                    "No high-risk transaction with anomaly score ≥ 80%.",
                    text_style
                )
            )


        else:

            story.append(
                Paragraph(
                    f"{len(high_risk_df)} transactions detected "
                    f"with anomaly score ≥ 80%.",
                    text_style
                )
            )


            # --------------------------------------------------------
            # Table header
            # --------------------------------------------------------

            table_data = [
                [
                    "Transaction ID",
                    "Customer ID",
                    "Amount ($)",
                    "Anomaly Score",
                    "Transaction Date"
                ]
            ]


            # --------------------------------------------------------
            # Table rows
            # --------------------------------------------------------

            for _, row in high_risk_df.iterrows():

                table_data.append([
                    str(row["transaction_id"]),

                    str(row["customer_id"]),

                    f"{float(row['amount']):,.2f}",

                    f"{float(row['anomaly_score']):.2f}%",

                    pd.to_datetime(
                        row["transaction_date"],
                        utc=True
                    ).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                ])


            # --------------------------------------------------------
            # Table
            # --------------------------------------------------------

            table = Table(
                table_data,
                colWidths=[
                    45 * mm,
                    45 * mm,
                    25 * mm,
                    35 * mm,
                    45 * mm
                ],
                repeatRows=1
            )


            table.setStyle(
                TableStyle([

                    (
                        "BACKGROUND",
                        (0, 0),
                        (-1, 0),
                        colors.HexColor("#E8ECF1")
                    ),

                    (
                        "TEXTCOLOR",
                        (0, 0),
                        (-1, 0),
                        colors.black
                    ),

                    (
                        "FONTNAME",
                        (0, 0),
                        (-1, 0),
                        "Helvetica-Bold"
                    ),

                    (
                        "FONTNAME",
                        (0, 1),
                        (-1, -1),
                        "Helvetica"
                    ),

                    (
                        "FONTSIZE",
                        (0, 0),
                        (-1, -1),
                        8
                    ),

                    (
                        "ALIGN",
                        (0, 0),
                        (-1, -1),
                        "CENTER"
                    ),

                    (
                        "VALIGN",
                        (0, 0),
                        (-1, -1),
                        "MIDDLE"
                    ),

                    (
                        "GRID",
                        (0, 0),
                        (-1, -1),
                        0.5,
                        colors.grey
                    ),

                    (
                        "TOPPADDING",
                        (0, 0),
                        (-1, -1),
                        6
                    ),

                    (
                        "BOTTOMPADDING",
                        (0, 0),
                        (-1, -1),
                        6
                    )

                ])
            )


            story.append(table)


        # ============================================================
        # 21. Build PDF
        # ============================================================

        doc.build(story)

        pdf_data = buffer.getvalue()

        buffer.close()


        # ============================================================
        # 22. Save report on ADLS
        # ============================================================

        report_directory = "anomaly_reports"

        report_path = (
            f"{report_directory}/{report_filename}"
        )

        report_file = file_system_client.get_file_client(
            report_path
        )

        report_file.upload_data(
            pdf_data,
            overwrite=True
        )


        # ============================================================
        # 23. Update checkpoint
        #     ONLY after successful PDF upload
        # ============================================================

        if len(new_transactions_df) > 0:

            new_checkpoint = (
                new_transactions_df["timestamp"].max()
            )

            checkpoint_file.upload_data(
                new_checkpoint.isoformat(),
                overwrite=True
            )

        # ============================================================
        # 24. Success response
        # ============================================================

        return func.HttpResponse(
            f"Report saved successfully: {report_path}",
            status_code=200
        )


    except Exception as e:

        return func.HttpResponse(
            f"ERROR: {type(e).__name__}: {str(e)}",
            status_code=500
        )