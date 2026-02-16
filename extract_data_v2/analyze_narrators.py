import json
import csv
from collections import Counter
import os

# --- إعدادات ---
INPUT_FILE = "Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json" 
OUTPUT_CSV = "Bukhari/narrators_stats.csv"

def analyze_narrators():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: File not found at {INPUT_FILE}")
        return

    print(f"📂 Loading data from {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 1. تجميع كل الأسماء
    all_names = []
    
    print("🔍 Extracting narrator names...")
    for hadith in data:
        # تأكد أن الحديث تمت معالجته وليس فيه خطأ
        if "chains" in hadith and isinstance(hadith["chains"], list):
            for chain in hadith["chains"]:
                for narrator in chain["narrators"]:
                    # تنظيف بسيط: إزالة المسافات الزائدة
                    raw_name = narrator["name"].strip()
                    if raw_name:
                        all_names.append(raw_name)

    # 2. حساب التكرارات
    name_counts = Counter(all_names)
    unique_count = len(name_counts)
    total_mentions = len(all_names)

    print(f"📊 Statistics:")
    print(f"   - Total Narrator Mentions: {total_mentions}")
    print(f"   - Unique Raw Names: {unique_count}")

    # 3. الحفظ في ملف CSV
    print(f"💾 Saving stats to {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Raw Name', 'Frequency', 'Suggested Canonical Name (Empty)'])  
        # العمود الثالث فارغ لتملأه أنت أو الموديل لاحقاً

        # نرتبهم من الأكثر تكراراً للأقل
        for name, count in name_counts.most_common():
            writer.writerow([name, count, ''])

    print("✅ Done! Check the CSV file.")
    
    # 4. معاينة سريعة لأكثر 10 أسماء تكراراً
    print("\n🏆 Top 10 Most Frequent Names (Targets for Normalization):")
    for name, count in name_counts.most_common(10):
        print(f"   - {name}: {count}")

if __name__ == "__main__":
    analyze_narrators()