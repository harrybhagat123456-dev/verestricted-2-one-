'use client';

import { useSyncExternalStore, useState } from 'react';
import { useQuery, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useParams } from 'next/navigation';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  Home,
  XCircle,
  BookOpen,
  Gauge,
  Trophy,
  TrendingUp,
  AlertCircle,
  Loader2,
  Moon,
  Sun,
} from 'lucide-react';
import { ScoreCard } from '@/components/dashboard/ScoreCard';
import { WrongQuestions } from '@/components/dashboard/WrongQuestions';
import { TopicBreakdown } from '@/components/dashboard/TopicBreakdown';
import { DifficultyAnalysis } from '@/components/dashboard/DifficultyAnalysis';
import { Leaderboard } from '@/components/dashboard/Leaderboard';
import { ImprovementTrend } from '@/components/dashboard/ImprovementTrend';
import { useTheme } from 'next-themes';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30000,
      retry: 1,
    },
  },
});

function DashboardContent() {
  const params = useParams();
  const secret = params.secret as string;
  const [activeTab, setActiveTab] = useState('scorecard');
  const { theme, setTheme } = useTheme();
  const mounted = useSyncExternalStore(
    () => () => {},
    () => true,
    () => false
  );

  // Verify secret
  const { data: verifyData, isLoading: verifying, isError: verifyError } = useQuery({
    queryKey: ['verify', secret],
    queryFn: async () => {
      const res = await fetch(`/api/verify?secret=${encodeURIComponent(secret)}`);
      if (!res.ok) throw new Error('Invalid dashboard link');
      return res.json();
    },
    enabled: !!secret,
  });

  const userId = verifyData?.user_id;

  // Fetch all data
  const { data: statsData, isLoading: statsLoading } = useQuery({
    queryKey: ['stats', userId],
    queryFn: async () => {
      const res = await fetch(`/api/stats?user_id=${userId}`);
      if (!res.ok) throw new Error('Failed to fetch stats');
      return res.json();
    },
    enabled: !!userId,
  });

  const { data: wrongData, isLoading: wrongLoading } = useQuery({
    queryKey: ['wrong', userId],
    queryFn: async () => {
      const res = await fetch(`/api/wrong?user_id=${userId}`);
      if (!res.ok) throw new Error('Failed to fetch wrong questions');
      return res.json();
    },
    enabled: !!userId,
  });

  const { data: topicsData, isLoading: topicsLoading } = useQuery({
    queryKey: ['topics', userId],
    queryFn: async () => {
      const res = await fetch(`/api/topics?user_id=${userId}`);
      if (!res.ok) throw new Error('Failed to fetch topics');
      return res.json();
    },
    enabled: !!userId,
  });

  const { data: difficultyData, isLoading: difficultyLoading } = useQuery({
    queryKey: ['difficulty', userId],
    queryFn: async () => {
      const res = await fetch(`/api/difficulty?user_id=${userId}`);
      if (!res.ok) throw new Error('Failed to fetch difficulty');
      return res.json();
    },
    enabled: !!userId,
  });

  const { data: leaderboardData, isLoading: leaderboardLoading } = useQuery({
    queryKey: ['leaderboard'],
    queryFn: async () => {
      const res = await fetch('/api/leaderboard');
      if (!res.ok) throw new Error('Failed to fetch leaderboard');
      return res.json();
    },
    enabled: !!userId,
  });

  const { data: trendData, isLoading: trendLoading } = useQuery({
    queryKey: ['trend', userId],
    queryFn: async () => {
      const res = await fetch(`/api/trend?user_id=${userId}`);
      if (!res.ok) throw new Error('Failed to fetch trend');
      return res.json();
    },
    enabled: !!userId,
  });

  // Loading state
  if (verifying) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto text-emerald-500 mb-4" />
          <p className="text-muted-foreground">Verifying dashboard link...</p>
        </div>
      </div>
    );
  }

  // Error state
  if (verifyError || (verifyData && verifyData.error)) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background p-4">
        <Card className="max-w-md w-full">
          <CardContent className="p-8 text-center">
            <AlertCircle className="h-12 w-12 mx-auto text-red-500 mb-4" />
            <h2 className="text-xl font-bold mb-2">Invalid Dashboard Link</h2>
            <p className="text-muted-foreground text-sm">
              This dashboard link doesn&apos;t appear to be valid. Please check the link from your Telegram bot and try again.
            </p>
          </CardContent>
        </Card>
      </div>
    );
  }

  const tabs = [
    { value: 'scorecard', label: 'Score', icon: Home },
    { value: 'wrong', label: 'Wrong', icon: XCircle },
    { value: 'topics', label: 'Topics', icon: BookOpen },
    { value: 'difficulty', label: 'Difficulty', icon: Gauge },
    { value: 'leaderboard', label: 'Rank', icon: Trophy },
    { value: 'trend', label: 'Trend', icon: TrendingUp },
  ];

  return (
    <div className="min-h-screen bg-background flex flex-col">
      {/* Header */}
      <header className="sticky top-0 z-40 bg-background/80 backdrop-blur-md border-b">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center justify-between">
          <div>
            <h1 className="text-base font-bold">Quiz Dashboard</h1>
            <p className="text-xs text-muted-foreground">
              {verifyData?.first_name || 'Loading...'}
            </p>
          </div>
          {mounted && (
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
              className="h-9 w-9"
            >
              {theme === 'dark' ? (
                <Sun className="h-4 w-4" />
              ) : (
                <Moon className="h-4 w-4" />
              )}
            </Button>
          )}
        </div>
      </header>

      {/* Desktop: Side tabs + content */}
      <div className="hidden md:flex max-w-4xl mx-auto w-full flex-1">
        {/* Side navigation */}
        <nav className="w-48 shrink-0 border-r p-4 space-y-1 sticky top-14 self-start">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <Button
                key={tab.value}
                variant={activeTab === tab.value ? 'secondary' : 'ghost'}
                className="w-full justify-start gap-2 h-10"
                onClick={() => setActiveTab(tab.value)}
              >
                <Icon className="h-4 w-4" />
                {tab.label}
              </Button>
            );
          })}
        </nav>

        {/* Content */}
        <main className="flex-1 p-6 min-h-0 overflow-y-auto">
          {activeTab === 'scorecard' && (
            <ScoreCard
              stats={statsData}
              trend={trendData}
              firstName={verifyData?.first_name || ''}
              username={verifyData?.username || ''}
              loading={statsLoading}
            />
          )}
          {activeTab === 'wrong' && (
            <WrongQuestions
              wrongQuestions={wrongData?.wrongQuestions}
              loading={wrongLoading}
            />
          )}
          {activeTab === 'topics' && (
            <TopicBreakdown
              topics={topicsData?.topics}
              loading={topicsLoading}
            />
          )}
          {activeTab === 'difficulty' && (
            <DifficultyAnalysis
              difficulties={difficultyData?.difficulties}
              loading={difficultyLoading}
            />
          )}
          {activeTab === 'leaderboard' && (
            <Leaderboard
              leaderboard={leaderboardData?.leaderboard}
              currentUserId={userId ?? null}
              loading={leaderboardLoading}
            />
          )}
          {activeTab === 'trend' && (
            <ImprovementTrend
              trend={trendData}
              loading={trendLoading}
            />
          )}
        </main>
      </div>

      {/* Mobile: Content + Bottom tabs */}
      <div className="md:hidden flex flex-col flex-1">
        <main className="flex-1 p-4 pb-20 overflow-y-auto">
          {activeTab === 'scorecard' && (
            <ScoreCard
              stats={statsData}
              trend={trendData}
              firstName={verifyData?.first_name || ''}
              username={verifyData?.username || ''}
              loading={statsLoading}
            />
          )}
          {activeTab === 'wrong' && (
            <WrongQuestions
              wrongQuestions={wrongData?.wrongQuestions}
              loading={wrongLoading}
            />
          )}
          {activeTab === 'topics' && (
            <TopicBreakdown
              topics={topicsData?.topics}
              loading={topicsLoading}
            />
          )}
          {activeTab === 'difficulty' && (
            <DifficultyAnalysis
              difficulties={difficultyData?.difficulties}
              loading={difficultyLoading}
            />
          )}
          {activeTab === 'leaderboard' && (
            <Leaderboard
              leaderboard={leaderboardData?.leaderboard}
              currentUserId={userId ?? null}
              loading={leaderboardLoading}
            />
          )}
          {activeTab === 'trend' && (
            <ImprovementTrend
              trend={trendData}
              loading={trendLoading}
            />
          )}
        </main>

        {/* Bottom Tab Navigation */}
        <nav className="fixed bottom-0 left-0 right-0 z-50 bg-background/95 backdrop-blur-md border-t safe-area-bottom">
          <div className="flex items-center justify-around px-1 py-1">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.value;
              return (
                <button
                  key={tab.value}
                  onClick={() => setActiveTab(tab.value)}
                  className={`flex flex-col items-center gap-0.5 py-1.5 px-2 min-w-[48px] min-h-[44px] justify-center rounded-lg transition-colors ${
                    isActive
                      ? 'text-emerald-600 dark:text-emerald-400'
                      : 'text-muted-foreground'
                  }`}
                >
                  <Icon className="h-4.5 w-4.5" />
                  <span className="text-[10px] font-medium">{tab.label}</span>
                </button>
              );
            })}
          </div>
        </nav>
      </div>
    </div>
  );
}

export default function DashboardPage() {
  return (
    <QueryClientProvider client={queryClient}>
      <DashboardContent />
    </QueryClientProvider>
  );
}
