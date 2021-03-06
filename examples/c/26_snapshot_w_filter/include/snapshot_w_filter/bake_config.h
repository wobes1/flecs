/*
                                   )
                                  (.)
                                  .|.
                                  | |
                              _.--| |--._
                           .-';  ;`-'& ; `&.
                          \   &  ;    &   &_/
                           |"""---...---"""|
                           \ | | | | | | | /
                            `---.|.|.|.---'

 * This file is generated by bake.lang.c for your convenience. Headers of
 * dependencies will automatically show up in this file. Include bake_config.h
 * in your main project file. Do not edit! */

#ifndef SNAPSHOT_W_FILTER_BAKE_CONFIG_H
#define SNAPSHOT_W_FILTER_BAKE_CONFIG_H

/* Headers of public dependencies */
#include <flecs.h>

/* Headers of private dependencies */
#ifdef SNAPSHOT_W_FILTER_IMPL
/* No dependencies */
#endif

/* Convenience macro for exporting symbols */
#ifndef SNAPSHOT_W_FILTER_STATIC
  #if SNAPSHOT_W_FILTER_IMPL && (defined(_MSC_VER) || defined(__MINGW32__))
    #define SNAPSHOT_W_FILTER_EXPORT __declspec(dllexport)
  #elif SNAPSHOT_W_FILTER_IMPL
    #define SNAPSHOT_W_FILTER_EXPORT __attribute__((__visibility__("default")))
  #elif defined _MSC_VER
    #define SNAPSHOT_W_FILTER_EXPORT __declspec(dllimport)
  #else
    #define SNAPSHOT_W_FILTER_EXPORT
  #endif
#else
  #define SNAPSHOT_W_FILTER_EXPORT
#endif

#endif

