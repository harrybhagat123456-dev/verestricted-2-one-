import { NextRequest, NextResponse } from 'next/server';
import { getDb } from '@/lib/mongodb';

export async function GET(request: NextRequest) {
  try {
    const userId = request.nextUrl.searchParams.get('user_id');
    if (!userId) {
      return NextResponse.json({ error: 'Missing user_id parameter' }, { status: 400 });
    }

    const db = await getDb();
    const answersCol = db.collection('dashboard_user_answers');

    const numUserId = Number(userId);

    const [attempted, correct, wrong, streakDocs, avgResponse, totalUsers] = await Promise.all([
      answersCol.countDocuments({ user_id: numUserId }),
      answersCol.countDocuments({ user_id: numUserId, is_correct: true }),
      answersCol.countDocuments({ user_id: numUserId, is_correct: false }),
      answersCol.find({ user_id: numUserId }, { sort: { answered_at: 1 }, projection: { is_correct: 1, answered_at: 1 } }).toArray(),
      answersCol.aggregate([
        { $match: { user_id: numUserId, response_time_seconds: { $exists: true, $ne: null } } },
        { $group: { _id: null, avg: { $avg: '$response_time_seconds' } } },
      ]).toArray(),
      answersCol.aggregate([
        { $group: { _id: '$user_id' } },
        { $count: 'total' },
      ]).toArray(),
    ]);

    // Calculate current streak and best streak
    let currentStreak = 0;
    let bestStreak = 0;
    let tempStreak = 0;

    for (let i = streakDocs.length - 1; i >= 0; i--) {
      if (streakDocs[i].is_correct) {
        tempStreak++;
        if (i === streakDocs.length - 1) {
          currentStreak = tempStreak;
        }
        bestStreak = Math.max(bestStreak, tempStreak);
      } else {
        if (i === streakDocs.length - 1) {
          currentStreak = 0;
        }
        tempStreak = 0;
      }
    }

    // Calculate rank
    const rankResult = await answersCol.aggregate([
      { $match: { is_correct: true } },
      { $group: { _id: '$user_id', correctCount: { $sum: 1 } } },
      { $sort: { correctCount: -1 } },
    ]).toArray();

    let rank = rankResult.length + 1;
    for (let i = 0; i < rankResult.length; i++) {
      if (rankResult[i]._id === numUserId) {
        rank = i + 1;
        break;
      }
    }

    const accuracy = attempted > 0 ? Math.round((correct / attempted) * 10000) / 100 : 0;
    const avgResponseTime = avgResponse.length > 0 ? Math.round(avgResponse[0].avg * 100) / 100 : 0;
    const totalUsersCount = totalUsers.length > 0 ? totalUsers[0].total : 0;

    return NextResponse.json({
      attempted,
      correct,
      wrong,
      accuracy,
      currentStreak,
      bestStreak,
      avgResponseTime,
      rank,
      totalUsers: totalUsersCount,
    });
  } catch (error) {
    console.error('Stats error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
