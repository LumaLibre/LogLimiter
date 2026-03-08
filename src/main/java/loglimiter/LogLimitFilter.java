package loglimiter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.filter.AbstractFilter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LogLimitFilter extends AbstractFilter {

    static final String FLUSH_MARKER_NAME = "LOG_LIMITER_FLUSH";
    private static final Marker FLUSH_MARKER = MarkerManager.getMarker(FLUSH_MARKER_NAME);

    private volatile long resetMillis;
    private volatile boolean active = true;

    // Shared across both the Log4j2 filter and the JUL filter
    final ConcurrentHashMap<String, MessageEntry> entries = new ConcurrentHashMap<>();

    public LogLimitFilter(long resetMillis) {
        super(Result.NEUTRAL, Result.DENY);
        this.resetMillis = resetMillis;
    }

    @Override
    public Result filter(LogEvent event) {
        if (!active) return Result.NEUTRAL;

        // Always pass through our own flush output to avoid recursion
        Marker marker = event.getMarker();
        if (marker != null && FLUSH_MARKER_NAME.equals(marker.getName())) {
            return Result.NEUTRAL;
        }

        String formatted = event.getMessage().getFormattedMessage();
        boolean allow = checkAndRecord(event.getLoggerName(), event.getLevel(), formatted);
        return allow ? Result.NEUTRAL : Result.DENY;
    }

    boolean checkAndRecord(String loggerName, Level level, String message) {
        String key = loggerName + '\0' + level.name() + '\0' + message;
        long now = System.currentTimeMillis();

        MessageEntry newEntry = new MessageEntry(loggerName, level, message, now);
        MessageEntry existing = entries.putIfAbsent(key, newEntry);
        if (existing == null) return true;

        long firstSeen = existing.firstSeenTime;
        if ((now - firstSeen) >= resetMillis) {
            existing.firstSeenTime = now;
            existing.count.set(0);
            return true;
        }

        existing.count.incrementAndGet();
        return false;
    }

    public void flush() {
        long now = System.currentTimeMillis();
        entries.forEach((key, entry) -> {
            long count = entry.count.getAndSet(0);
            if (count > 0) {
                LogManager.getLogger(entry.loggerName)
                        .log(entry.level, FLUSH_MARKER,
                                "[Suppressed x{}] {}", count, entry.formattedMessage);
            }
            // Clean up entries that haven't been seen for 2x the reset window
            if (count == 0 && (now - entry.firstSeenTime) >= resetMillis * 2L) {
                entries.remove(key, entry);
            }
        });
    }

    public void activate(long newResetMillis) {
        this.resetMillis = newResetMillis;
        this.active = true;
    }

    public void deactivate() {
        this.active = false;
    }

    public void clear() {
        entries.clear();
    }

    static final class MessageEntry {
        final String loggerName;
        final Level level;
        final String formattedMessage;
        volatile long firstSeenTime;
        final AtomicLong count = new AtomicLong(0);

        MessageEntry(String loggerName, Level level, String formattedMessage, long firstSeenTime) {
            this.loggerName = loggerName;
            this.level = level;
            this.formattedMessage = formattedMessage;
            this.firstSeenTime = firstSeenTime;
        }
    }
}
