import json
import csv
from collections import defaultdict, Counter
import os

# --- إعدادات ---
INPUT_JSON = "Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json"
OUTPUT_CSV = "Bukhari/ambiguous_narrators_for_llm.csv"

# قائمة الأسماء التي نريد فك لبسها (كلمة واحدة أو أسماء شائعة جداً)
TARGET_NAMES = {
    'سفيان', 'حماد', 'إسماعيل', 'هشام', 'يحيى', 'إبراهيم', 
    'عمرو', 'عبيد الله', 'عبد الله', 'أبيه', 'أبي', 
    'شعبة', 'سعيد', 'الحكم', 'موسى', 'مسلم', 'خالد',
    'الليث', 'مالك', 'ثابت', 'قتادة', 'سليمان'
}

def is_ambiguous(name):
    # نعتبر الاسم غامضاً إذا كان في القائمة المستهدفة
    # أو إذا كان مكوناً من كلمة واحدة (ولا يبدأ بـ عبد/أبو/ابن)
    name = name.strip()
    if name in TARGET_NAMES:
        return True
    
    parts = name.split()
    if len(parts) == 1 and name not in ['النبي', 'رسول الله']:
        return True
        
    return False

def extract_contexts():
    if not os.path.exists(INPUT_JSON):
        print("❌ File not found.")
        return

    print(f"📂 Loading data from {INPUT_JSON}...")
    with open(INPUT_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Dictionary format:
    # { "Ambiguous Name": Counter({ "Student Name 1": count, "Student Name 2": count }) }
    context_map = defaultdict(Counter)

    print("🔍 Extracting relationships...")
    for hadith in data:
        if "chains" in hadith:
            for chain in hadith["chains"]:
                narrators = chain["narrators"]
                
                for i, narrator in enumerate(narrators):
                    raw_name = narrator["name"].strip()
                    
                    if is_ambiguous(raw_name):
                        # تحديد التلميذ (الراوي السابق في القائمة)
                        # القائمة عادة: [التلميذ (الأحدث), الشيخ (الأقدم), ...]
                        if i > 0:
                            student_name = narrators[i-1]["name"].strip()
                        else:
                            # إذا كان هو الأول في القائمة، فالتلميذ هو "البخاري"
                            student_name = "البخاري (بداية السند)"
                        
                        context_map[raw_name][student_name] += 1

    # تصدير البيانات إلى CSV
    print(f"💾 Saving report to {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        # الرؤوس: الاسم الغامض، التلميذ، التكرار، (خانة فارغة للـ LLM)
        writer.writerow(['Ambiguous Name', 'Student (Narrator From)', 'Frequency', 'Full Name (LLM Prediction)'])
        
        # ترتيب النتائج: الأسماء الأكثر تكراراً أولاً
        sorted_names = sorted(context_map.keys(), key=lambda k: sum(context_map[k].values()), reverse=True)
        
        for name in sorted_names:
            students = context_map[name]
            # نأخذ أهم التلاميذ (الذين رووا عنه أكثر من مرة) لتقليل الضوضاء
            for student, count in students.most_common():
                if count >= 2: # تجاهل الحالات النادرة جداً لتوفير التوكيز للـ LLM
                    writer.writerow([name, student, count, ''])

    print("✅ Done! File ready for LLM processing.")
    print("👉 Upload 'ambiguous_narrators_for_llm.csv' to Gemini/ChatGPT with the prompt provided.")

if __name__ == "__main__":
    extract_contexts()