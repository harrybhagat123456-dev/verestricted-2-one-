'use client';

import { Card, CardContent } from '@/components/ui/card';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Trophy, Medal, Crown } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';
import { motion } from 'framer-motion';

interface LeaderboardEntry {
  rank: number;
  user_id: number;
  username: string;
  first_name: string;
  total: number;
  correct: number;
  accuracy: number;
}

interface LeaderboardProps {
  leaderboard: LeaderboardEntry[] | null;
  currentUserId: number | null;
  loading: boolean;
}

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.04 },
  },
};

const item = {
  hidden: { opacity: 0, x: -10 },
  show: { opacity: 1, x: 0 },
};

function getRankIcon(rank: number) {
  if (rank === 1) return <Crown className="h-5 w-5 text-yellow-500" />;
  if (rank === 2) return <Medal className="h-5 w-5 text-gray-400" />;
  if (rank === 3) return <Medal className="h-5 w-5 text-amber-600" />;
  return null;
}

function getRankBg(rank: number) {
  if (rank === 1) return 'bg-yellow-50 dark:bg-yellow-950/30 border-yellow-200 dark:border-yellow-800';
  if (rank === 2) return 'bg-gray-50 dark:bg-gray-900/30 border-gray-200 dark:border-gray-700';
  if (rank === 3) return 'bg-amber-50 dark:bg-amber-950/30 border-amber-200 dark:border-amber-800';
  return '';
}

export function Leaderboard({ leaderboard, currentUserId, loading }: LeaderboardProps) {
  if (loading) {
    return (
      <div className="space-y-3">
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} className="h-16 rounded-xl" />
        ))}
      </div>
    );
  }

  if (!leaderboard || leaderboard.length === 0) {
    return (
      <Card>
        <CardContent className="p-8 text-center">
          <Trophy className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
          <h3 className="text-lg font-semibold mb-1">No Leaderboard Data</h3>
          <p className="text-sm text-muted-foreground">
            No users have answered any questions yet.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      {/* Top 3 Podium */}
      {leaderboard.length >= 3 && (
        <div className="grid grid-cols-3 gap-2">
          {/* 2nd place */}
          <Card className="mt-6">
            <CardContent className="p-3 text-center">
              <Medal className="h-5 w-5 mx-auto text-gray-400 mb-1" />
              <Avatar className="h-10 w-10 mx-auto mb-1">
                <AvatarFallback className="bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300 text-sm">
                  {leaderboard[1].first_name?.charAt(0)?.toUpperCase() || '?'}
                </AvatarFallback>
              </Avatar>
              <p className="text-xs font-medium truncate">{leaderboard[1].first_name || 'Unknown'}</p>
              <p className="text-xs text-emerald-500 font-bold">{leaderboard[1].accuracy}%</p>
            </CardContent>
          </Card>
          {/* 1st place */}
          <Card className="border-yellow-300 dark:border-yellow-700">
            <CardContent className="p-3 text-center">
              <Crown className="h-6 w-6 mx-auto text-yellow-500 mb-1" />
              <Avatar className="h-12 w-12 mx-auto mb-1">
                <AvatarFallback className="bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300 text-base">
                  {leaderboard[0].first_name?.charAt(0)?.toUpperCase() || '?'}
                </AvatarFallback>
              </Avatar>
              <p className="text-xs font-bold truncate">{leaderboard[0].first_name || 'Unknown'}</p>
              <p className="text-xs text-emerald-500 font-bold">{leaderboard[0].accuracy}%</p>
            </CardContent>
          </Card>
          {/* 3rd place */}
          <Card className="mt-8">
            <CardContent className="p-3 text-center">
              <Medal className="h-5 w-5 mx-auto text-amber-600 mb-1" />
              <Avatar className="h-10 w-10 mx-auto mb-1">
                <AvatarFallback className="bg-amber-100 text-amber-700 dark:bg-amber-900 dark:text-amber-300 text-sm">
                  {leaderboard[2].first_name?.charAt(0)?.toUpperCase() || '?'}
                </AvatarFallback>
              </Avatar>
              <p className="text-xs font-medium truncate">{leaderboard[2].first_name || 'Unknown'}</p>
              <p className="text-xs text-emerald-500 font-bold">{leaderboard[2].accuracy}%</p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Full List */}
      <ScrollArea className="max-h-[calc(100vh-380px)]">
        <motion.div
          variants={container}
          initial="hidden"
          animate="show"
          className="space-y-2"
        >
          {leaderboard.map((entry) => {
            const isCurrentUser = currentUserId !== null && entry.user_id === currentUserId;
            const rankBg = getRankBg(entry.rank);

            return (
              <motion.div key={entry.user_id} variants={item}>
                <Card
                  className={`transition-all ${
                    isCurrentUser
                      ? 'border-emerald-300 dark:border-emerald-700 bg-emerald-50/50 dark:bg-emerald-950/20 ring-1 ring-emerald-200 dark:ring-emerald-800'
                      : rankBg
                  }`}
                >
                  <CardContent className="p-3">
                    <div className="flex items-center gap-3">
                      <div className="w-8 text-center shrink-0">
                        {getRankIcon(entry.rank) || (
                          <span className="text-sm font-bold text-muted-foreground">
                            #{entry.rank}
                          </span>
                        )}
                      </div>
                      <Avatar className="h-8 w-8 shrink-0">
                        <AvatarFallback className="text-xs">
                          {entry.first_name?.charAt(0)?.toUpperCase() || '?'}
                        </AvatarFallback>
                      </Avatar>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-1.5">
                          <p className="text-sm font-medium truncate">
                            {entry.first_name || 'Unknown'}
                          </p>
                          {isCurrentUser && (
                            <Badge className="bg-emerald-100 text-emerald-700 dark:bg-emerald-900 dark:text-emerald-300 text-[10px] border-0 px-1.5 py-0">
                              You
                            </Badge>
                          )}
                        </div>
                        {entry.username && (
                          <p className="text-xs text-muted-foreground truncate">
                            @{entry.username}
                          </p>
                        )}
                      </div>
                      <div className="text-right shrink-0">
                        <p className="text-sm font-bold">
                          {entry.correct}/{entry.total}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {entry.accuracy}%
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </motion.div>
            );
          })}
        </motion.div>
      </ScrollArea>
    </div>
  );
}
