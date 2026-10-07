# --- Monitoring: alert on pipeline job execution failures ---
resource "google_monitoring_notification_channel" "pipeline_alerts_email" {
  display_name = "SP500 pipeline alerts"
  type         = "email"
  labels = {
    email_address = var.alert_notification_email
  }
}

resource "google_monitoring_alert_policy" "pipeline_job_failures" {
  display_name = "SP500 pipeline Cloud Run Job execution failures"
  combiner     = "OR"

  conditions {
    display_name = "Job execution failed"
    condition_threshold {
      # Metric verified against the live descriptor
      # (projects/<p>/metricDescriptors/run.googleapis.com/job/completed_execution_count):
      # DELTA / INT64, monitored resource cloud_run_job, label `result`
      # ("succeeded" | "failed"). An execution is only counted once, after
      # max_retries is exhausted, so one point == one genuinely failed run.
      filter          = "resource.type = \"cloud_run_job\" AND resource.labels.job_name = \"${google_cloud_run_v2_job.sp500_pipeline.name}\" AND metric.type = \"run.googleapis.com/job/completed_execution_count\" AND metric.labels.result = \"failed\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM" # DELTA counter: sum failures per window
      }
      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "86400s" # one daily run: stale incidents close before the next run
  }

  documentation {
    mime_type = "text/markdown"
    content   = "The daily sp500-pipeline Cloud Run Job failed (after its single retry). Check `gcloud run jobs executions list --job=sp500-pipeline --region=${var.region}` and the execution logs in Cloud Logging, plus the latest `pipeline_runs` row in BigQuery."
  }

  notification_channels = [google_monitoring_notification_channel.pipeline_alerts_email.name]
}
