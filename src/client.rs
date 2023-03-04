use std::error::Error;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Record {
    #[serde(rename(deserialize = "ACCOUNTID"))]
    pub account_id: String,

    #[serde(rename(deserialize = "CODE"))]
    pub code: String,

    #[serde(rename(deserialize = "CODE_MA"))]
    pub code_ma: String,

    #[serde(rename(deserialize = "PERIOADA"))]
    pub epoch: String,

    #[serde(rename(deserialize = "CONTRACT_LENGTH"))]
    pub contract_length: Option<i32>,

    #[serde(rename(deserialize = "CONTRACT_EXPIRATION_DATE"))]
    pub contract_expiration_date: String,

    #[serde(rename(deserialize = "CREANTE_REST"))]
    pub remaining_receivables: Option<i32>,

    #[serde(rename(deserialize = "CONCURENTI"))]
    pub competitors: Option<i32>,

    #[serde(rename(deserialize = "INCIDENTE"))]
    pub incidents: Option<i32>,
    #[serde(rename(deserialize = "STATUT_CONTRACT"))]
    pub contract_status: String,

    #[serde(rename(deserialize = "SOLICITARI_REZILIERE"))]
    pub termination_request: Option<i32>, // avg

    #[serde(rename(deserialize = "NET_PARAMS"))]
    pub net_params: String,

    #[serde(rename(deserialize = "PRET_ABON"))]
    pub subscription_price: String,

    #[serde(rename(deserialize = "QUALITY_PERCEPTION"))]
    pub quality_perception: String,

    #[serde(rename(deserialize = "COST_PERCEPTION"))]
    pub cost_perception: String,

    #[serde(rename(deserialize = "TECHNOLOGY"))]
    pub technology: String,

    #[serde(rename(deserialize = "IPTV_STB_QUANTITY"))]
    pub iptv_stb_quantity: String,

    #[serde(rename(deserialize = "INET_PACK"))]
    pub inet_pack: String,

    #[serde(rename(deserialize = "QNT_APELARI"))]
    pub qnt_calls: String,

    #[serde(rename(deserialize = "QNT_PORT_REZ"))]
    pub qnt_port_res: String,

    #[serde(rename(deserialize = "AVG_PERCEPTION"))]
    pub avg_perception: String,

    #[serde(rename(deserialize = "QNT_SUSP"))]
    pub qnt_susp: String,

    #[serde(rename(deserialize = "QNT_INCEDENT"))]
    pub qnt_incident: Option<i32>,

    #[serde(rename(deserialize = "LUNA_SUSPENDARI"))]
    pub dissolution_month: String,

    #[serde(rename(deserialize = "LUNA_INCIDENT"))]
    pub incident_month: String,

    #[serde(rename(deserialize = "LUNA_APEL"))]
    pub call_month: String,

    #[serde(rename(deserialize = "LUNI_DATOR"))]
    pub debth_month: String,

    #[serde(rename(deserialize = "PERIOADA_ACHITARII"))]
    pub payment_period: String,

    #[serde(rename(deserialize = "SUMA_ACHITARII"))]
    pub payment_sum: String,

    #[serde(rename(deserialize = "MULTIPLAY"))]
    pub multiply: String,

    #[serde(rename(deserialize = "CNT_SERVICII"))]
    pub cnt_services: String,

    #[serde(rename(deserialize = "IS_CHURN"))]
    pub is_churn: Option<u8>,

    #[serde(rename(deserialize = "CHURN_DATE"))]
    pub churn_date: String,
}
