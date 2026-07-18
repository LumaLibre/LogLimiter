package loglimiter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.filter.AbstractFilter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LogLimitFilter extends AbstractFilter {

    static final String FLUSH_MARKER_NAME = "LOG_LIMITER_FLUSH";
    private static final Marker FLUSH_MARKER = MarkerManager.getMarker(FLUSH_MARKER_NAME);

    private volatile long resetMillis;
    private volatile boolean active = true;

    // Similarity threshold as a fraction (0.0 - 1.0)
    private volatile double similarityThreshold;
    private volatile int messagesBeforeBlock;

    final ConcurrentHashMap<String, List<MessageEntry>> buckets = new ConcurrentHashMap<>();

    public LogLimitFilter(long resetMillis, double similarityThreshold, int messagesBeforeBlock) {
        super(Result.NEUTRAL, Result.DENY);
        this.resetMillis = resetMillis;
        this.similarityThreshold = similarityThreshold;
        this.messagesBeforeBlock = messagesBeforeBlock;
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
        String bucketKey = loggerName + '\0' + level.name();
        long now = System.currentTimeMillis();

        List<MessageEntry> bucket = buckets.computeIfAbsent(bucketKey, k -> new ArrayList<>());

        synchronized (bucket) {
            MessageEntry match = null;
            for (MessageEntry entry : bucket) {
                if (isSimilar(entry.formattedMessage, message)) {
                    match = entry;
                    break;
                }
            }

            if (match == null) {
                MessageEntry entry = new MessageEntry(loggerName, level, message, now);
                entry.allowed = 1;
                bucket.add(entry);
                return true;
            }

            // Window elapsed: treat as new and let it through again
            if ((now - match.firstSeenTime) >= resetMillis) {
                match.firstSeenTime = now;
                match.count.set(0);
                match.allowed = 1;
                return true;
            }

            // Still within the window: allow up to messagesBeforeBlock through
            if (match.allowed < messagesBeforeBlock) {
                match.allowed++;
                return true;
            }

            match.count.incrementAndGet();
            return false;
        }
    }

    public void flush() {
        long now = System.currentTimeMillis();
        buckets.forEach((bucketKey, bucket) -> {
            synchronized (bucket) {
                Iterator<MessageEntry> it = bucket.iterator();
                while (it.hasNext()) {
                    MessageEntry entry = it.next();
                    long count = entry.count.getAndSet(0);
                    if (count > 0) {
                        LogManager.getLogger(entry.loggerName)
                                .log(entry.level, FLUSH_MARKER,
                                        "[Suppressed x{}] {}", count, entry.formattedMessage);
                    }
                    // Clean up entries that haven't been seen for 2x the reset window
                    if (count == 0 && (now - entry.firstSeenTime) >= resetMillis * 2L) {
                        it.remove();
                    }
                }
            }
            // Drop empty buckets so keys for gone loggers don't accumulate
            buckets.computeIfPresent(bucketKey, (k, b) -> b.isEmpty() ? null : b);
        });
    }

    private boolean isSimilar(String a, String b) {
        if (a.equals(b)) return true;
        int lenA = a.length();
        int lenB = b.length();
        int maxLen = Math.max(lenA, lenB);
        if (maxLen == 0) return true;

        // Upper bound: similarity can never exceed 1 - |lenA-lenB|/maxLen, since
        // the edit distance is at least the length difference. Skip the (costly)
        // full computation when even that bound falls short of the threshold.
        double bestPossible = 1.0 - (double) Math.abs(lenA - lenB) / maxLen;
        if (bestPossible < similarityThreshold) return false;

        int distance = levenshtein(a, b);
        double similarity = 1.0 - (double) distance / maxLen;
        return similarity >= similarityThreshold;
    }

    private static int levenshtein(String a, String b) {
        int lenA = a.length();
        int lenB = b.length();
        int[] prev = new int[lenB + 1];
        int[] curr = new int[lenB + 1];
        for (int j = 0; j <= lenB; j++) prev[j] = j;

        for (int i = 1; i <= lenA; i++) {
            curr[0] = i;
            char ca = a.charAt(i - 1);
            for (int j = 1; j <= lenB; j++) {
                int cost = (ca == b.charAt(j - 1)) ? 0 : 1;
                curr[j] = Math.min(
                        Math.min(curr[j - 1] + 1, prev[j] + 1),
                        prev[j - 1] + cost);
            }
            int[] tmp = prev;
            prev = curr;
            curr = tmp;
        }
        return prev[lenB];
    }

    public void activate(long newResetMillis, double newSimilarityThreshold, int newMessagesBeforeBlock) {
        this.resetMillis = newResetMillis;
        this.similarityThreshold = newSimilarityThreshold;
        this.messagesBeforeBlock = newMessagesBeforeBlock;
        this.active = true;
    }

    public void reconfigure(long newResetMillis, double newSimilarityThreshold, int newMessagesBeforeBlock) {
        this.resetMillis = newResetMillis;
        this.similarityThreshold = newSimilarityThreshold;
        this.messagesBeforeBlock = newMessagesBeforeBlock;
    }

    public void deactivate() {
        this.active = false;
    }

    public void clear() {
        buckets.clear();
    }

    static final class MessageEntry {
        final String loggerName;
        final Level level;
        final String formattedMessage;
        volatile long firstSeenTime;
        int allowed;
        final AtomicLong count = new AtomicLong(0);

        MessageEntry(String loggerName, Level level, String formattedMessage, long firstSeenTime) {
            this.loggerName = loggerName;
            this.level = level;
            this.formattedMessage = formattedMessage;
            this.firstSeenTime = firstSeenTime;
        }
    }
}
