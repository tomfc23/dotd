#!/usr/bin/env python3
"""
DOTD (Deal of the Day) Data Collector - Single Run Version
Fetches betting data from Real.vg API and appends it to a JSON file.
"""

import requests
import json
from datetime import datetime, timezone
from pathlib import Path
import pytz


class DOTDCollector:
    def __init__(self, api_url: str, output_dir: str = "dotd_data"):
        self.api_url = api_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def _get_est_timezone(self):
        return pytz.timezone('US/Eastern')

    def _parse_lock_time(self, lock_time_str: str):
        try:
            if '+' in lock_time_str or lock_time_str.endswith('Z'):
                return datetime.fromisoformat(lock_time_str.replace('Z', '+00:00'))
            return datetime.fromisoformat(lock_time_str)
        except:
            return None

    def _calculate_implied_probability(self, odds_str: str) -> float:
        try:
            odds = int(odds_str.replace('+', ''))
            if odds > 0:
                return 100 / (odds + 100)
            return abs(odds) / (abs(odds) + 100)
        except:
            return 0.0

    def _fetch_api_data(self):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json'
            }
            r = requests.get(self.api_url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"❌ Error fetching API data: {e}")
            return None

    def _save_data(self, snapshot_data):
        try:
            file_path = self.output_dir / "dotd_all_snapshots.json"
            all_snapshots = []

            if file_path.exists():
                try:
                    with open(file_path, 'r') as f:
                        existing = json.load(f)
                        if isinstance(existing, list):
                            all_snapshots = existing
                        else:
                            all_snapshots = [existing]
                except:
                    pass

            all_snapshots.append(snapshot_data)

            with open(file_path, 'w') as f:
                json.dump(all_snapshots, f, indent=2)

            print(f"✅ Updated {file_path} with snapshot #{len(all_snapshots)}")
        except Exception as e:
            print(f"❌ Error saving data: {e}")

    def _process_snapshot(self, raw_data):
        if not raw_data:
            return None

        poll_data = raw_data.get('poll', raw_data)
        if not poll_data or 'options' not in poll_data:
            return None

        snapshot_time = datetime.now(timezone.utc)
        total_votes = sum(o.get('count', 0) for o in poll_data['options'])

        event_info = {
            'date': snapshot_time.date().isoformat(),
            'total_teams': len(poll_data['options']),
            'sport': poll_data.get('sport', 'unknown'),
            'snapshot_timestamp': snapshot_time.isoformat(),
            'poll_id': poll_data.get('id'),
            'is_daily_dog': poll_data.get('additionalInfo', {}).get('isDailyDog', False),
            'total_votes_at_snapshot': total_votes
        }

        lock_times = [
            self._parse_lock_time(o.get('locksAt', ''))
            for o in poll_data['options'] if o.get('locksAt')
        ]
        if lock_times:
            event_info['first_game_start_time'] = min(lock_times).isoformat()

        teams = []
        for o in poll_data['options']:
            vote_count = o.get('count', 0)
            vote_pct = (vote_count / total_votes * 100) if total_votes else 0

            t = {
                'team_id': o.get('id'),
                'team_identifier': o.get('label'),
                'american_odds': o.get('odds'),
                'current_vote_percentage': round(vote_pct, 2),
                'vote_count': vote_count,
                'rank': o.get('priority', 0),
                'game_id': o.get('additionalInfo', {}).get('gameId'),
                'team_db_id': o.get('additionalInfo', {}).get('teamId'),
                'multiplier': o.get('multiplier', 1.0),
                'is_locked': o.get('isLocked', False),
                'game_lock_time': o.get('locksAt'),
                'implied_win_probability': round(self._calculate_implied_probability(o.get('odds', '+100')), 4),
                'rank_percentile': round(o.get('priority', 0) / len(poll_data['options']), 4)
            }

            if t['game_lock_time']:
                lock_time = self._parse_lock_time(t['game_lock_time'])
                if lock_time:
                    hours_until = (lock_time - snapshot_time).total_seconds() / 3600
                    t['hours_until_game_starts'] = round(hours_until, 2)

            teams.append(t)

        teams.sort(key=lambda x: x['rank'])
        if teams:
            leader_votes = teams[0]['vote_count']
            leader_pct = teams[0]['current_vote_percentage']
            for t in teams:
                t['votes_behind_leader'] = leader_votes - t['vote_count']
                t['percentage_behind_leader'] = round(leader_pct - t['current_vote_percentage'], 2)

        return {
            'collection_info': {
                'api_url': self.api_url,
                'collection_time': snapshot_time.isoformat(),
                'data_format_version': '1.0'
            },
            'event_info': event_info,
            'teams': teams
        }

    def run(self):
        print("📡 Running single DOTD snapshot collection...")
        raw_data = self._fetch_api_data()
        snapshot = self._process_snapshot(raw_data)
        if snapshot:
            self._save_data(snapshot)
        else:
            print("⚠️ No snapshot collected.")


if __name__ == "__main__":
    API_URL = "https://api.real.vg/polls/270943"
    OUTPUT_DIR = "dotd_data"
    DOTDCollector(API_URL, OUTPUT_DIR).run()
