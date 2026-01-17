import logging
from typing import Dict, Any, List
from src.config.settings import settings

log = logging.getLogger('notifications')

class NotificationService:
    """Сервис для отправки уведомлений о статусе пайплайна."""
    
    def __init__(self):
        # В будущем здесь можно инициализировать Telegram-бота или SMTP клиент
        pass

    def send_summary(self, run_id: str, status: str, stats: Dict[str, Any], quality_issues: List[Dict[str, Any]]):
        """Отправляет итоговую сводку о запуске."""
        
        emoji = "✅" if status == 'success' else "❌"
        title = f"{emoji} ELT Run Summary: {status.upper()}"
        
        message = [
            title,
            f"Run ID: {run_id}",
            f"Tables processed: {stats.get('tables_processed', 0)}",
            f"Rows synced: {stats.get('total_rows_synced', 0)}",
            f"Validation errors: {stats.get('validation_errors', 0)}",
        ]
        
        if quality_issues:
            message.append("\n⚠️ Data Quality Issues:")
            for issue in quality_issues[:10]: # Ограничиваем количество
                sev_icon = "🛑" if issue['severity'] == 'critical' else "⚠"
                message.append(f"  {sev_icon} {issue['table']}: {issue['message']}")
            
            if len(quality_issues) > 10:
                message.append(f"  ...and {len(quality_issues) - 10} more.")

        # Пока просто выводим в лог с высоким уровнем, чтобы было заметно
        full_msg = "\n".join(message)
        log.info(f"\n{'='*40}\n{full_msg}\n{'='*40}")
        
    def send_alert(self, title: str, message: str, severity: str = 'error'):
        """Отправляет мгновенный алерт о сбое."""
        icon = "🚨" if severity == 'error' else "⚠"
        log.error(f"{icon} {title}: {message}")
