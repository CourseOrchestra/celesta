package ru.curs.celesta.score.discovery;

import ru.curs.celesta.score.io.Resource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class CombinedScoreDiscovery implements ScoreDiscovery {

    private final List<ScoreDiscovery> discoveryList;

    public CombinedScoreDiscovery(ScoreDiscovery... discoveries) {
        discoveryList = new ArrayList<>();
        for (ScoreDiscovery discovery : discoveries) {
            discoveryList.add(discovery);
        }
    }

    @Override
    public Set<Resource> discoverScore() {
        Set<Resource> result = new HashSet<>();
        for (ScoreDiscovery discovery : discoveryList) {
            result.addAll(discovery.discoverScore());
        }
        return result;
    }
}
