import { NextRequest, NextResponse } from 'next/server';
import { getDb } from '@/lib/mongodb';

export async function GET(request: NextRequest) {
  try {
    const userId = request.nextUrl.searchParams.get('user_id');
    if (!userId) {
      return NextResponse.json({ error: 'Missing user_id parameter' }, { status: 400 });
    }

    const db = await getDb();
    const numUserId = Number(userId);

    // Last 30 days trend
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    const trend = await db.collection('dashboard_user_answers').aggregate([
      { $match: { user_id: numUserId, answered_at: { $gte: thirtyDaysAgo } } },
      { $group: {
        _id: { $dateToString: { format: '%Y-%m-%d', date: '$answered_at' } },
        total: { $sum: 1 },
        correct: { $sum: { $cond: ['$is_correct', 1, 0] } },
      }},
      { $addFields: {
        accuracy: { $multiply: [{ $divide: ['$correct', '$total'] }, 100] },
      }},
      { $sort: { _id: 1 } },
    ]).toArray();

    const formatted = trend.map((t) => ({
      date: t._id,
      total: t.total,
      correct: t.correct,
      accuracy: Math.round(t.accuracy * 100) / 100,
    }));

    // Week-by-week comparison
    const weekComparison = await db.collection('dashboard_user_answers').aggregate([
      { $match: { user_id: numUserId, answered_at: { $gte: thirtyDaysAgo } } },
      { $group: {
        _id: {
          year: { $year: '$answered_at' },
          week: { $week: '$answered_at' },
        },
        total: { $sum: 1 },
        correct: { $sum: { $cond: ['$is_correct', 1, 0] } },
      }},
      { $addFields: {
        accuracy: { $multiply: [{ $divide: ['$correct', '$total'] }, 100] },
      }},
      { $sort: { '_id.year': 1, '_id.week': 1 } },
    ]).toArray();

    const weeklyFormatted = weekComparison.map((w) => ({
      week: `W${w._id.week}`,
      year: w._id.year,
      total: w.total,
      correct: w.correct,
      accuracy: Math.round(w.accuracy * 100) / 100,
    }));

    return NextResponse.json({ daily: formatted, weekly: weeklyFormatted });
  } catch (error) {
    console.error('Trend error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
