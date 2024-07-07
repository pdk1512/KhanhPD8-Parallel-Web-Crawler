package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

public final class CrawInternalTask extends RecursiveTask<Void> {
    private final String url;
    private final Instant deadline;
    private final int maxDepth;
    private final ConcurrentHashMap<String, Integer> counts;
    private final ConcurrentSkipListSet<String> visitedUrls;
    private final PageParserFactory parserFactory;
    private final Clock clock;
    private final List<Pattern> ignoredUrls;

    private CrawInternalTask(String url,
                            Instant deadline,
                            int maxDepth,
                            ConcurrentHashMap<String, Integer> counts,
                            ConcurrentSkipListSet<String> visitedUrls,
                            PageParserFactory parserFactory,
                            Clock clock,
                            List<Pattern> ignoredUrls) {
        this.url = url;
        this.deadline = deadline;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.parserFactory = parserFactory;
        this.clock = clock;
        this.ignoredUrls = ignoredUrls;
    }
    @Override
    protected Void compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
            return null;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return null;
            }
        }
        if(!visitedUrls.add(url)) {
            return null;
        }
        PageParser.Result result = parserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            if (counts.containsKey(e.getKey())) {
                counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
            } else {
                counts.put(e.getKey(), e.getValue());
            }
        }
        List<CrawInternalTask> crawInternalTasks = result.getLinks().stream()
                .map(link -> new CrawInternalTask(
                        link,
                        deadline,
                        maxDepth - 1,
                        counts,
                        visitedUrls,
                        parserFactory,
                        clock,
                        ignoredUrls
                )).toList();
        invokeAll(crawInternalTasks);
        return null;
    }

    public static final class Builder {
        private String url;
        private Instant deadline;
        private int maxDepth;
        private ConcurrentHashMap<String, Integer> counts;
        private ConcurrentSkipListSet<String> visitedUrls;
        private PageParserFactory parserFactory;
        private Clock clock;
        private List<Pattern> ignoredUrls;
        public Builder setUrl(String url) {
            this.url = url;
            return this;
        }
        public Builder setDeadline(Instant deadline) {
            this.deadline = deadline;
            return this;
        }
        public Builder setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }
        public Builder setCounts(ConcurrentHashMap<String, Integer> counts) {
            this.counts = counts;
            return this;
        }
        public Builder setVisitedUrls(ConcurrentSkipListSet<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }
        public Builder setParserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }
        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }
        public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }
        public CrawInternalTask build() {
            return new CrawInternalTask(
                    url,
                    deadline,
                    maxDepth,
                    counts,
                    visitedUrls,
                    parserFactory,
                    clock,
                    ignoredUrls
                    );
        }
    }
}
