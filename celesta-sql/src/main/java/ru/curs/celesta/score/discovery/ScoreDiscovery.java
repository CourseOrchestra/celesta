package ru.curs.celesta.score.discovery;

import ru.curs.celesta.score.io.Resource;

import java.util.Set;

/**
 * Score discovery interface.
 */
public interface ScoreDiscovery {

    /**
     * Discovers grains in the score.
     *
     * @return  a set of resources pointing to grain scripts of the score.
     */
    Set<Resource> discoverScore();

}
