package ru.curs.celesta.score.discovery;

import java.util.Set;

import ru.curs.celesta.score.io.Resource;

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
