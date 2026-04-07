/**
 * BigQuery Dashboard API
 * Deployed on Cloud Run — queries cogent-summer-486217-r1.boost_dev_synthetic.syn_contacts
 * and returns normalized Contact[] JSON to the React dashboard.
 */

const express = require('express')
const cors = require('cors')
const { BigQuery } = require('@google-cloud/bigquery')

const app = express()
const PORT = process.env.PORT || 8080
const PROJECT = 'cogent-summer-486217-r1'
const TABLE = '`cogent-summer-486217-r1.boost_dev_synthetic.Contacts`'

// ─── CORS ─────────────────────────────────────────────────────────────────────
// Restrict to your company's domain in production.
// Set the ALLOWED_ORIGIN env var in Cloud Run to your hosted dashboard URL.
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*'
app.use(cors({ origin: allowedOrigin }))
app.use(express.json())

// ─── BigQuery client ──────────────────────────────────────────────────────────
// On Cloud Run, authentication is handled automatically via the service account.
// No key file needed.
const bq = new BigQuery({ projectId: PROJECT })

// ─── Column name normalizer ───────────────────────────────────────────────────
// BigQuery may store column names with spaces, underscores, or mixed case
// depending on how the CRM data was imported. This mapper handles all variants.
function col(row, ...candidates) {
  for (const name of candidates) {
    if (row[name] !== undefined) return row[name] ?? null
  }
  return null
}

function toNumber(val) {
  if (val === null || val === undefined || val === '') return null
  const n = Number(val)
  return isNaN(n) ? null : n
}

function toStr(val) {
  if (val === null || val === undefined) return ''
  // BigQuery DATE/TIMESTAMP objects have a value property
  if (typeof val === 'object' && val.value !== undefined) return String(val.value)
  return String(val)
}

function toBool(val) {
  if (val === null || val === undefined || val === '') return null
  if (typeof val === 'boolean') return val
  const s = String(val).toLowerCase()
  if (s === 'true' || s === 'yes' || s === '1') return true
  if (s === 'false' || s === 'no' || s === '0') return false
  return null
}

function mapRow(row) {
  return {
    contactId:                    toStr(row.contact_id),
    firstName:                    toStr(row.first_name),
    lastName:                     toStr(row.last_name),
    phone:                        toStr(row.phone),
    email:                        toStr(row.email),
    created:                      toStr(row.created),
    lastActivity:                 toStr(row.last_activity),
    state:                        toStr(row.state),
    householdSize:                toNumber(row.household_size),
    existingPlanCarrier:          toStr(row.existing_plan_carrier),
    existingPlanPremium:          toNumber(row.existing_plan_premium),
    suggestedCarrier:             toStr(row.suggested_carrier),
    suggestedPlanName:            toStr(row.suggested_plan_name),
    suggestedPlanEffectiveDate:   toStr(row.suggested_plan_effective_date),
    suggestedPlanPremium:         toNumber(row.suggested_plan_premium),
    suggestedPlanGrossPremium:    toNumber(row.suggested_plan_gross_premium),
    digiBACAConvertedIdentifier:  toStr(row.digi_baca_converted_identifier),
    salesTagsDropDown:            toStr(row.sales_tags_drop_down),
    actionNeededItem:             toStr(row.action_needed_item_drop_down),
    primaryDMINonCoverage:        toStr(row.primary_dmi_non_coverage),
    primaryDMICoverage:           toStr(row.primary_dmi_coverage),
    sepOEUsedOnApplication:       toStr(row.sep_oe_used_on_application),
    customerApplicationId:        toStr(row.customer_application_id),
    effectiveDate:                toStr(row.effective_date),
    annualIncomeUsedOnApp:        toNumber(row.annual_income_used_on_app),
    householdSizeUsedOnApp:       toNumber(row.household_size_used_on_app),
    selectedPlanIssuerName:       toStr(row.selected_plan_issuer_name),
    selectedPlanName:             toStr(row.selected_plan_name),
    selectedPlanPremiumWithCredit:toNumber(row.selected_plan_premium_w_credit),
    selectedPlanPremium:          toNumber(row.selected_plan_premium),
    submissionDate:               toStr(row.submission_date),
    householdIncomeEst2026:       toNumber(row.household_income_est_2026),
    primaryDob:                   toStr(row.primary_dob),
    primaryPostalCode:            toStr(row.primary_postal_code),
    primaryCounty:                toStr(row.primary_county),
    deviceType:                   toStr(row.device_type),
    primaryGender:                toStr(row.primary_gender),
    primaryUsCitizen:             toBool(row.primary_us_citizen),
    currentInsurance:             toStr(row.current_insurance),
    spouseDateOfBirth:            toStr(row.spouse_dob),
    householdSize2026:            toNumber(row.household_size_2026),
    medicaidOutreachCount:        toNumber(row.medicaid_outreach_count) ?? 0,
    status:                       toStr(row.status),
    updated:                      toStr(row.updated),
    converted:                    toBool(row.converted) === true ? 'Yes' : 'No',
    resubmissionCount:            toNumber(row.resubmission_count) ?? 0,
    retentionAgent:               toStr(row.retention_agent),
    originalAssignedAgent:        toStr(row.original_assigned_agent),
    originalRetentionAgent:       toStr(row.original_retention_agent),
    temporaryRetentionAgent:      toStr(row.temporary_retention_agent),
    pendingConversionDate:        toStr(row.pending_conversion_date),
    dmiProofUploadedDate:         toStr(row.dmi_proof_uploaded_date),
    customerServiceAgent:         toStr(row.customer_service_agent),
    selectedPlanMetalLevel:       toStr(row.selected_plan_metal_level),
    selectedPlanType:             toStr(row.selected_plan_type),
    primaryDobAge:                toNumber(row.primary_dob_age),
    segment:                      toStr(row.segment),
    utmSource:                    toStr(row.utm_source),
    utmCampaign:                  toStr(row.utm_campaign),
    utmKeyword:                   toStr(row.utm_keyword),
    utmMatchtype:                 toStr(row.utm_matchtype),
    utmMedium:                    toStr(row.utm_medium),
    utmDevice:                    toStr(row.utm_device),
    utmLocation:                  toStr(row.utm_location),
    fbclid:                       toStr(row.fbclid),
    clickId:                      toStr(row.click_id),
    gclickid:                     toStr(row.gclickid),
    campaignId:                   toStr(row.campaign_id),
    adGroupId:                    toStr(row.ad_group_id),
    adId:                         toStr(row.ad_id),
    vendorSource:                 toStr(row.vendor_source),
    dataVariant:                  toStr(row.data_variant),
    affsub:                       toStr(row.affsub),
    affsub1:                      toStr(row.affsub1),
    affsub2:                      toStr(row.affsub2),
    affsub3:                      toStr(row.affsub3),
    subId:                        toStr(row.sub_id),
    utmGhl:                       toStr(row.utm_ghl),
    shortFormAttribution:         toStr(row.short_form_attribution),
    additionalServices:           toStr(row.additional_services),
    companyName:                  toStr(row.company_name),
    opportunities:                toStr(row.opportunities),
    tldStatus:                    toStr(row.tld_status),
    postalCode:                   toStr(row.postal_code),
    sfFormReceived:               toStr(row.sf_form_received),
    outreachSentCount:            toNumber(row.outreach_sent_count) ?? 0,
    opportunityDistribution:      toStr(row.opportunity_distribution),

    // ─── Document Collection ─────────────────────────────────────────────────
    documentCollectionResolved:            toStr(row.document_collection_resolved),
    documentCollectionFormReceived:        toStr(row.document_collection_form_received),
    documentCollectionResponseReceived:    toStr(row.document_collection_response_received),
    documentCollectionTriggerLinkClicked:  toStr(row.document_collection_trigger_link_clicked),
    documentCollectionInboundCall:         toStr(row.document_collection_inbound_call),
    documentCollectionOutreachSentCount:   toNumber(row.document_collection_outreach_sent_count) ?? 0,
    documentCollectionOutreachSentDate:    toStr(row.document_collection_outreach_sent_date),
    documentCollectionSplit:               toStr(row.document_collection_split),

    // ─── SF Remarketing ──────────────────────────────────────────────────────
    sfRemarketingResolved:            toStr(row.sf_remarketing_resolved),
    sfRemarketingFormReceived:        toStr(row.sf_remarketing_form_received),
    sfRemarketingResponseReceived:    toStr(row.sf_remarketing_response_received),
    sfRemarketingTriggerLinkClicked:  toStr(row.sf_remarketing_trigger_link_clicked),
    sfRemarketingInboundCall:         toStr(row.sf_remarketing_inbound_call),
    sfRemarketingOutreachSentCount:   toNumber(row.sf_remarketing_outreach_sent_count) ?? 0,
    sfRemarketingOutreachSentDate:    toStr(row.sf_remarketing_outreach_sent_date),
    sfRemarketingSplit:               toStr(row.sf_remarketing_split),
    sfOutreachSentCount:              toNumber(row.sf_outreach_sent_count) ?? 0,
    sfOutreachSentDate:               toStr(row.sf_outreach_sent_date),
    sfSplit:                          toStr(row.sf_split),

    // ─── Landline ─────────────────────────────────────────────────────────────
    landlineResolved:            toStr(row.landline_resolved),
    landlineFormReceived:        toStr(row.landline_form_received),
    landlineResponseReceived:    toStr(row.landline_response_received),
    landlineTriggerLinkClicked:  toStr(row.landline_trigger_link_clicked),
    landlineInboundCall:         toStr(row.landline_inbound_call),
    landlineOutreachSentCount:   toNumber(row.landline_outreach_sent_count) ?? 0,
    landlineOutreachSentDate:    toStr(row.landline_outreach_sent_date),
    landlineSplit:               toStr(row.landline_split),

    // ─── Lander Mismatch ─────────────────────────────────────────────────────
    landerMismatchResolved:            toStr(row.lander_mismatch_resolved),
    landerMismatchResponseReceived:    toStr(row.lander_mismatch_response_received),
    landerMismatchTriggerLinkClicked:  toStr(row.lander_mismatch_trigger_link_clicked),
    landerMismatchInboundCall:         toStr(row.lander_mismatch_inbound_call),
    landerMismatchOutreachSentCount:   toNumber(row.lander_mismatch_outreach_sent_count) ?? 0,
    landerMismatchOutreachSentDate:    toStr(row.lander_mismatch_outreach_sent_date),
    landerMismatchSplit:               toStr(row.lander_mismatch_split),

    // ─── Cancelled Client ────────────────────────────────────────────────────
    cancelledClientResolved:            toStr(row.cancelled_client_resolved),
    cancelledClientFormReceived:        toStr(row.cancelled_client_form_received),
    cancelledClientResponseReceived:    toStr(row.cancelled_client_response_received),
    cancelledClientTriggerLinkClicked:  toStr(row.cancelled_client_trigger_link_clicked),
    cancelledClientInboundCall:         toStr(row.cancelled_client_inbound_call),
    cancelledClientOutreachSentCount:   toNumber(row.cancelled_client_outreach_sent_count) ?? 0,
    cancelledClientOutreachSentDate:    toStr(row.cancelled_client_outreach_sent_date),
    cancelledClientOutreachSentDate2:   toStr(row.cancelled_client_outreach_sent_date_2),
    cancelledClientSplit:               toStr(row.cancelled_client_split),

    // ─── Qualification ───────────────────────────────────────────────────────
    qualificationResolved:            toStr(row.qualification_resolved),
    qualificationFormReceived:        toStr(row.qualification_form_received),
    qualificationResponseReceived:    toStr(row.qualification_response_received),
    qualificationTriggerLinkClicked:  toStr(row.qualification_trigger_link_clicked),
    qualificationInboundCall:         toStr(row.qualification_inbound_call),
    qualificationOutreachSentCount:   toNumber(row.qualification_outreach_sent_count) ?? 0,
    qualificationOutreachSentDate:    toStr(row.qualification_outreach_sent_date),
    qualificationSplit:               toStr(row.qualification_split),

    // ─── DMI ─────────────────────────────────────────────────────────────────
    dmiPostSaleOutreachSentCount:  toNumber(row.dmi_post_sale_outreach_sent_count) ?? 0,
    dmiPreSaleOutreachSentCount:   toNumber(row.dmi_pre_sale_outreach_sent_count) ?? 0,

    // ─── AOR Recovery ────────────────────────────────────────────────────────
    aorRecoveryOutreachSentCount:  toNumber(row.aor_recovery_outreach_sent_count) ?? 0,

    // ─── Misc Retention ──────────────────────────────────────────────────────
    medicaidRemarketing:                  toStr(row.medicaid_remarketing),
    medicaidRemarketingOutreachSentCount: toNumber(row.medicaid_remarketing_outreach_sent_count) ?? 0,
    premiumCollectionFormReceived:        toStr(row.premium_collection_form_received),
    premiumCollectionOutreachSentCount:   toNumber(row.premium_collection_outreach_sent_count) ?? 0,
    premiumCollectionOutreachSentDate:    toStr(row.premium_collection_outreach_sent_date),
    premiumCollectionSplit:               toStr(row.premium_collection_split),
    assistedLinkOutreachSentCount:        toNumber(row.assisted_link_outreach_sent_count) ?? 0,
    updateSpouseDependentSsn:             toStr(row.update_spouse_dependent_ssn),
    oeUnpaidBinderOutreachSentCount:      toNumber(row.oe_unpaid_binder_outreach_sent_count) ?? 0,
    otherPartyOutreachSentCount:          toNumber(row.other_party_outreach_sent_count) ?? 0,
  }
}

// ─── Date range filter (applied in SQL for efficiency) ────────────────────────
function dateFilter(range) {
  const field = 'COALESCE(Created, created)'
  switch (range) {
    case '7d':  return `DATE(${field}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)`
    case '30d': return `DATE(${field}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)`
    case '90d': return `DATE(${field}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`
    case 'ytd': return `EXTRACT(YEAR FROM DATE(${field})) = EXTRACT(YEAR FROM CURRENT_DATE())`
    case '12m': return `DATE(${field}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)`
    default:    return '1=1'
  }
}

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ status: 'ok' }))

app.get('/contacts', async (req, res) => {
  const range = req.query.range || '12m'

  try {
    const query = `
      SELECT *
      FROM ${TABLE}
      WHERE ${dateFilter(range)}
      ORDER BY COALESCE(Created, created) DESC
      LIMIT 5000
    `
    const [rows] = await bq.query({ query, location: 'US' })
    const contacts = rows.map(mapRow)
    res.json({ contacts, count: contacts.length })
  } catch (err) {
    console.error('BigQuery error:', err)
    res.status(500).json({ error: err.message })
  }
})

// ─── Debug: inspect raw column names (disable in production) ─────────────────
app.get('/debug/columns', async (_req, res) => {
  if (process.env.NODE_ENV === 'production') {
    return res.status(404).json({ error: 'Not found' })
  }
  try {
    const [rows] = await bq.query({
      query: `SELECT * FROM ${TABLE} LIMIT 1`,
      location: 'US',
    })
    const columns = rows.length > 0 ? Object.keys(rows[0]) : []
    res.json({ columns })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.listen(PORT, () => {
  console.log(`BigQuery API running on port ${PORT}`)
})
