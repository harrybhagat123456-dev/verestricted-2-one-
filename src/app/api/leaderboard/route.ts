import { NextResponse } from 'next/server';
import { getDb } from '@/lib/mongodb';

export async function GET() {
  try {
    const db = await getDb();

    const leaderboard = await db.collection('dashboard_user_answers').aggregate([
      { $group: {
        _id: '$user_id',
        username: { $first: '$username' },
        first_name: { $first: '$first_name' },
        total: { $sum: 1 },
        correct: { $sum: { $cond: ['$is_correct', 1, 0] } },
      }},
      { $addFields: {
        accuracy: { $multiply: [{ $divide: ['$correct', '$total'] }, 100] },
      }},
      { $sort: { correct: -1, accuracy: -1 } },
      { $limit: 50 },
    ]).toArray();

    const formatted = leaderboard.map((u, i) => ({
      rank: i + 1,
      user_id: u._id,
      username: u.username || '',
      first_name: u.first_name || '',
      total: u.total,
      correct: u.correct,
      accuracy: Math.round(u.accuracy * 100) / 100,
    }));

    return NextResponse.json({ leaderboard: formatted });
  } catch (error) {
    console.error('Leaderboard error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
