# ⚽ Premier League Live Control Room

A real-time football analytics dashboard built with **Streamlit**, **AWS Athena**, and **Plotly**. This application ingests live Premier League data, processes it via an AWS Glue Data Lake, and visualizes key metrics like Goals, Assists, xG (Threat), and Form.

## 🚀 Features

-   **Cloud-Native Architecture**: Fetches data directly from an **AWS S3 Data Lake** using **Serverless Athena Queries**.
-   **Hybrid Authentication**: Smart security logic that works seamlessly on **Streamlit Cloud** (using `st.secrets`) and **Local Machines** (using `~/.aws/credentials`) without changing code.
-   **Live Data Deduplication**: Advanced SQL Window Functions (`ROW_NUMBER()`) ensure only the latest snapshot is visualized, filtering out older or corrupted data ingestion batches.
-   **Interactive Dashboard**:
    -   Compare Players & Teams.
    -   Analyze "Threat" (xG) vs "Creativity".
    -   Track Injury Reports & Availability.

## 🛠️ Tech Stack

-   **Frontend**: Streamlit (Python)
-   **Visualization**: Plotly Express
-   **Backend/Data**: AWS Athena, AWS Wrangler (`awswrangler`), AWS Glue
-   **Infrastructure**: AWS S3 (Parquet Data Lake)

## ⚙️ Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/football-dashboard.git
cd football-dashboard
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. AWS Credentials Setup
The app uses a **Hybrid Authentication** bridge:
-   **Local Development**: It automatically uses your system's AWS credentials (`~/.aws/credentials`). No extra config needed if you have AWS CLI configured.
-   **Streamlit Cloud**: Add your secrets in the Streamlit Dashboard.

## ☁️ Deploying to Streamlit Cloud

1.  Push this code to **GitHub**.
2.  Go to [Streamlit Community Cloud](https://share.streamlit.io/) and connect your repository.
3.  In the **App Settings** -> **Secrets** menu, add your AWS keys:

```toml
[aws]
access_key_id = "YOUR_ACCESS_KEY"
secret_access_key = "YOUR_SECRET_KEY"
region = "eu-north-1"
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
