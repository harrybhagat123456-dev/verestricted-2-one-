'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  BarChart3,
  Target,
  Trophy,
  BookOpen,
  ArrowRight,
  Link as LinkIcon,
  Moon,
  Sun,
  Zap,
} from 'lucide-react';
import { useTheme } from 'next-themes';
import { motion } from 'framer-motion';

const features = [
  {
    icon: Target,
    title: 'Performance Stats',
    description: 'Track your accuracy, streaks, and response times',
    color: 'text-emerald-500',
    bg: 'bg-emerald-50 dark:bg-emerald-950/40',
  },
  {
    icon: BookOpen,
    title: 'Topic Analysis',
    description: 'Find your strengths and weaknesses by topic',
    color: 'text-blue-500',
    bg: 'bg-blue-50 dark:bg-blue-950/40',
  },
  {
    icon: Trophy,
    title: 'Leaderboard',
    description: 'See how you rank against other quiz participants',
    color: 'text-yellow-500',
    bg: 'bg-yellow-50 dark:bg-yellow-950/40',
  },
  {
    icon: BarChart3,
    title: 'Improvement Trends',
    description: 'Visualize your progress over time',
    color: 'text-purple-500',
    bg: 'bg-purple-50 dark:bg-purple-950/40',
  },
];

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.1 },
  },
};

const item = {
  hidden: { opacity: 0, y: 20 },
  show: { opacity: 1, y: 0 },
};

export default function LandingPage() {
  const [secret, setSecret] = useState('');
  const router = useRouter();
  const { theme, setTheme } = useTheme();

  const handleNavigate = () => {
    const trimmed = secret.trim();
    if (trimmed) {
      // If they pasted a full URL, extract the secret
      if (trimmed.includes('/d/')) {
        const parts = trimmed.split('/d/');
        router.push(`/d/${parts[parts.length - 1]}`);
      } else {
        router.push(`/d/${trimmed}`);
      }
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleNavigate();
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      {/* Header */}
      <header className="border-b">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Zap className="h-5 w-5 text-emerald-500" />
            <span className="font-bold text-sm">Quiz Dashboard</span>
          </div>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
            className="h-9 w-9"
          >
            {theme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </Button>
        </div>
      </header>

      {/* Hero */}
      <main className="flex-1 flex flex-col">
        <div className="max-w-2xl mx-auto w-full px-4 py-12 md:py-20 flex-1 flex flex-col justify-center">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="text-center mb-10"
          >
            <div className="inline-flex items-center gap-2 bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300 px-3 py-1 rounded-full text-xs font-medium mb-6">
              <Zap className="h-3 w-3" />
              Powered by Telegram Bot
            </div>
            <h1 className="text-3xl md:text-4xl font-bold mb-3 tracking-tight">
              Quiz Performance
              <br />
              <span className="text-emerald-500">Dashboard</span>
            </h1>
            <p className="text-muted-foreground text-sm md:text-base max-w-md mx-auto leading-relaxed">
              Track your quiz performance, analyze your strengths and weaknesses,
              and compete with others on the leaderboard.
            </p>
          </motion.div>

          {/* Secret Input */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="mb-12"
          >
            <Card>
              <CardContent className="p-4">
                <div className="flex items-center gap-2 mb-3">
                  <LinkIcon className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-medium">Enter your dashboard link</span>
                </div>
                <div className="flex gap-2">
                  <Input
                    placeholder="Paste your secret or dashboard URL"
                    value={secret}
                    onChange={(e) => setSecret(e.target.value)}
                    onKeyDown={handleKeyDown}
                    className="flex-1 h-11"
                  />
                  <Button
                    onClick={handleNavigate}
                    disabled={!secret.trim()}
                    className="h-11 px-4 bg-emerald-600 hover:bg-emerald-700"
                  >
                    <ArrowRight className="h-4 w-4" />
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  Get your personal link from the Telegram bot. Format: /d/your-secret
                </p>
              </CardContent>
            </Card>
          </motion.div>

          {/* Features Grid */}
          <motion.div
            variants={container}
            initial="hidden"
            animate="show"
            className="grid grid-cols-2 gap-3"
          >
            {features.map((feature) => {
              const Icon = feature.icon;
              return (
                <motion.div key={feature.title} variants={item}>
                  <Card className="h-full">
                    <CardContent className={`p-4 ${feature.bg} rounded-xl`}>
                      <Icon className={`h-6 w-6 ${feature.color} mb-2`} />
                      <h3 className="text-sm font-semibold mb-1">{feature.title}</h3>
                      <p className="text-xs text-muted-foreground leading-relaxed">
                        {feature.description}
                      </p>
                    </CardContent>
                  </Card>
                </motion.div>
              );
            })}
          </motion.div>
        </div>

        {/* Footer */}
        <footer className="border-t py-4">
          <div className="max-w-2xl mx-auto px-4 text-center">
            <p className="text-xs text-muted-foreground">
              Telegram Quiz Bot Dashboard &middot; Track your learning progress
            </p>
          </div>
        </footer>
      </main>
    </div>
  );
}
