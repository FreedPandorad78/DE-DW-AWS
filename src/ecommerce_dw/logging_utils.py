import logging


class BatchLog:
    """Cada línea lleva lote y tarea, para saber qué lote y qué paso falló."""

    def __init__(self, task, batch_id):
        self._log = logging.getLogger("airflow.task")
        self.task, self.batch_id = task, batch_id

    def _fmt(self, msg):
        return f"[lote={self.batch_id}] [tarea={self.task}] {msg}"

    def info(self, msg): self._log.info(self._fmt(msg))
    def warning(self, msg): self._log.warning(self._fmt(msg))
    def error(self, msg): self._log.error(self._fmt(msg))
