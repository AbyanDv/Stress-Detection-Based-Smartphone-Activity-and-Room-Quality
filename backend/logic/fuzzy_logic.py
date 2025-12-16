"""
fuzzy_logic.py
Implementasi Fuzzy Inference System (Mamdani) untuk Stress Detection
dengan 81 aturan fuzzy (3x3x3x3 = 81 kombinasi)
"""

import numpy as np

class FuzzyMamdani:
    def __init__(self):
        # Definisi Membership Functions untuk setiap input
        self.mf_screen = {
            'low': (0, 0, 2, 4),
            'medium': (3, 5, 7, 9),
            'high': (8, 12, 24, 24)
        }
        
        self.mf_temp = {
            'cold': (15, 15, 18, 22),
            'normal': (20, 24, 26, 28),
            'hot': (26, 30, 35, 35)
        }
        
        self.mf_humid = {
            'low': (30, 30, 40, 50),
            'medium': (45, 55, 65, 75),
            'high': (70, 80, 90, 90)
        }
        
        self.mf_aq = {
            'good': (0, 0, 0.5, 1.5),
            'moderate': (1.0, 2.0, 3.0, 3.5),
            'poor': (3.0, 4.0, 5.0, 5.0)
        }
        
        # Membership Functions untuk Output (Stress Level)
        self.mf_stress = {
            'very_low': (0, 0, 10, 25),
            'low': (15, 25, 35, 45),
            'medium': (35, 45, 55, 65),
            'high': (55, 65, 75, 85),
            'very_high': (75, 85, 100, 100)
        }
        
        # 81 Aturan Fuzzy (3^4 = 81)
        # Format: (screen, temp, humid, aq) -> stress_output
        self.rules = self._generate_rules()
    
    def _generate_rules(self):
        """Generate 81 fuzzy rules"""
        rules = []
        
        screen_levels = ['low', 'medium', 'high']
        temp_levels = ['cold', 'normal', 'hot']
        humid_levels = ['low', 'medium', 'high']
        aq_levels = ['good', 'moderate', 'poor']
        
        # Matriks keputusan untuk menentukan output stress
        # Logika: screen_time tinggi + kondisi buruk = stress tinggi
        rule_matrix = {
            # Format: (screen, temp, humid, aq): output
            
            # === SCREEN LOW (0-4 jam) ===
            ('low', 'cold', 'low', 'good'): 'low',
            ('low', 'cold', 'low', 'moderate'): 'low',
            ('low', 'cold', 'low', 'poor'): 'medium',
            ('low', 'cold', 'medium', 'good'): 'very_low',
            ('low', 'cold', 'medium', 'moderate'): 'low',
            ('low', 'cold', 'medium', 'poor'): 'low',
            ('low', 'cold', 'high', 'good'): 'low',
            ('low', 'cold', 'high', 'moderate'): 'low',
            ('low', 'cold', 'high', 'poor'): 'medium',
            
            ('low', 'normal', 'low', 'good'): 'very_low',
            ('low', 'normal', 'low', 'moderate'): 'very_low',
            ('low', 'normal', 'low', 'poor'): 'low',
            ('low', 'normal', 'medium', 'good'): 'very_low',
            ('low', 'normal', 'medium', 'moderate'): 'very_low',
            ('low', 'normal', 'medium', 'poor'): 'low',
            ('low', 'normal', 'high', 'good'): 'very_low',
            ('low', 'normal', 'high', 'moderate'): 'low',
            ('low', 'normal', 'high', 'poor'): 'low',
            
            ('low', 'hot', 'low', 'good'): 'low',
            ('low', 'hot', 'low', 'moderate'): 'low',
            ('low', 'hot', 'low', 'poor'): 'medium',
            ('low', 'hot', 'medium', 'good'): 'low',
            ('low', 'hot', 'medium', 'moderate'): 'low',
            ('low', 'hot', 'medium', 'poor'): 'medium',
            ('low', 'hot', 'high', 'good'): 'low',
            ('low', 'hot', 'high', 'moderate'): 'medium',
            ('low', 'hot', 'high', 'poor'): 'medium',
            
            # === SCREEN MEDIUM (5-9 jam) ===
            ('medium', 'cold', 'low', 'good'): 'low',
            ('medium', 'cold', 'low', 'moderate'): 'medium',
            ('medium', 'cold', 'low', 'poor'): 'medium',
            ('medium', 'cold', 'medium', 'good'): 'low',
            ('medium', 'cold', 'medium', 'moderate'): 'low',
            ('medium', 'cold', 'medium', 'poor'): 'medium',
            ('medium', 'cold', 'high', 'good'): 'medium',
            ('medium', 'cold', 'high', 'moderate'): 'medium',
            ('medium', 'cold', 'high', 'poor'): 'high',
            
            ('medium', 'normal', 'low', 'good'): 'low',
            ('medium', 'normal', 'low', 'moderate'): 'low',
            ('medium', 'normal', 'low', 'poor'): 'medium',
            ('medium', 'normal', 'medium', 'good'): 'low',
            ('medium', 'normal', 'medium', 'moderate'): 'medium',
            ('medium', 'normal', 'medium', 'poor'): 'medium',
            ('medium', 'normal', 'high', 'good'): 'medium',
            ('medium', 'normal', 'high', 'moderate'): 'medium',
            ('medium', 'normal', 'high', 'poor'): 'high',
            
            ('medium', 'hot', 'low', 'good'): 'medium',
            ('medium', 'hot', 'low', 'moderate'): 'medium',
            ('medium', 'hot', 'low', 'poor'): 'high',
            ('medium', 'hot', 'medium', 'good'): 'medium',
            ('medium', 'hot', 'medium', 'moderate'): 'medium',
            ('medium', 'hot', 'medium', 'poor'): 'high',
            ('medium', 'hot', 'high', 'good'): 'medium',
            ('medium', 'hot', 'high', 'moderate'): 'high',
            ('medium', 'hot', 'high', 'poor'): 'high',
            
            # === SCREEN HIGH (10-24 jam) ===
            ('high', 'cold', 'low', 'good'): 'medium',
            ('high', 'cold', 'low', 'moderate'): 'high',
            ('high', 'cold', 'low', 'poor'): 'high',
            ('high', 'cold', 'medium', 'good'): 'medium',
            ('high', 'cold', 'medium', 'moderate'): 'high',
            ('high', 'cold', 'medium', 'poor'): 'high',
            ('high', 'cold', 'high', 'good'): 'high',
            ('high', 'cold', 'high', 'moderate'): 'high',
            ('high', 'cold', 'high', 'poor'): 'very_high',
            
            ('high', 'normal', 'low', 'good'): 'medium',
            ('high', 'normal', 'low', 'moderate'): 'high',
            ('high', 'normal', 'low', 'poor'): 'high',
            ('high', 'normal', 'medium', 'good'): 'high',
            ('high', 'normal', 'medium', 'moderate'): 'high',
            ('high', 'normal', 'medium', 'poor'): 'very_high',
            ('high', 'normal', 'high', 'good'): 'high',
            ('high', 'normal', 'high', 'moderate'): 'high',
            ('high', 'normal', 'high', 'poor'): 'very_high',
            
            ('high', 'hot', 'low', 'good'): 'high',
            ('high', 'hot', 'low', 'moderate'): 'high',
            ('high', 'hot', 'low', 'poor'): 'very_high',
            ('high', 'hot', 'medium', 'good'): 'high',
            ('high', 'hot', 'medium', 'moderate'): 'very_high',
            ('high', 'hot', 'medium', 'poor'): 'very_high',
            ('high', 'hot', 'high', 'good'): 'high',
            ('high', 'hot', 'high', 'moderate'): 'very_high',
            ('high', 'hot', 'high', 'poor'): 'very_high',
        }
        
        return rule_matrix
    
    def trapezoid_membership(self, x, params):
        """
        Fungsi keanggotaan trapezoid
        params: (a, b, c, d)
        """
        a, b, c, d = params
        
        if x <= a or x >= d:
            return 0.0
        elif a < x <= b:
            return (x - a) / (b - a) if (b - a) != 0 else 1.0
        elif b < x < c:
            return 1.0
        elif c <= x < d:
            return (d - x) / (d - c) if (d - c) != 0 else 1.0
        else:
            return 0.0
    
    def fuzzification(self, screen, temp, humid, aq):
        """
        Tahap 1: Fuzzifikasi - mengubah input crisp menjadi fuzzy
        Menghitung derajat keanggotaan untuk setiap input
        """
        fuzzy_values = {
            'screen': {},
            'temp': {},
            'humid': {},
            'aq': {}
        }
        
        # Fuzzifikasi Screen Time
        for label, params in self.mf_screen.items():
            fuzzy_values['screen'][label] = self.trapezoid_membership(screen, params)
        
        # Fuzzifikasi Temperature
        for label, params in self.mf_temp.items():
            fuzzy_values['temp'][label] = self.trapezoid_membership(temp, params)
        
        # Fuzzifikasi Humidity
        for label, params in self.mf_humid.items():
            fuzzy_values['humid'][label] = self.trapezoid_membership(humid, params)
        
        # Fuzzifikasi Air Quality
        for label, params in self.mf_aq.items():
            fuzzy_values['aq'][label] = self.trapezoid_membership(aq, params)
        
        return fuzzy_values
    
    def inference(self, fuzzy_values):
        """
        Tahap 2: Inference - menerapkan 81 aturan fuzzy
        Menggunakan operator MIN untuk AND
        """
        activated_rules = []
        
        for rule_key, output_label in self.rules.items():
            screen_label, temp_label, humid_label, aq_label = rule_key
            
            # Ambil nilai fuzzy untuk setiap antecedent
            screen_val = fuzzy_values['screen'][screen_label]
            temp_val = fuzzy_values['temp'][temp_label]
            humid_val = fuzzy_values['humid'][humid_label]
            aq_val = fuzzy_values['aq'][aq_label]
            
            # Operator AND menggunakan MIN
            firing_strength = min(screen_val, temp_val, humid_val, aq_val)
            
            if firing_strength > 0:
                activated_rules.append({
                    'rule': rule_key,
                    'output': output_label,
                    'strength': firing_strength
                })
        
        return activated_rules
    
    def defuzzification(self, activated_rules):
        """
        Tahap 3: Defuzzifikasi - mengubah fuzzy output menjadi crisp
        Menggunakan metode Centroid (Center of Gravity)
        """
        if not activated_rules:
            return 50.0  # Default medium stress
        
        # Agregasi menggunakan MAX untuk setiap output label
        aggregated = {}
        for rule in activated_rules:
            output_label = rule['output']
            strength = rule['strength']
            
            if output_label not in aggregated:
                aggregated[output_label] = strength
            else:
                aggregated[output_label] = max(aggregated[output_label], strength)
        
        # Hitung centroid
        numerator = 0.0
        denominator = 0.0
        
        # Diskretisasi output (0-100) dengan step 1
        for x in range(0, 101):
            membership_at_x = 0.0
            
            # Untuk setiap output fuzzy set yang ter-aktivasi
            for output_label, activation_level in aggregated.items():
                params = self.mf_stress[output_label]
                membership = self.trapezoid_membership(x, params)
                
                # Clipping dengan activation level
                clipped_membership = min(membership, activation_level)
                membership_at_x = max(membership_at_x, clipped_membership)
            
            numerator += x * membership_at_x
            denominator += membership_at_x
        
        if denominator == 0:
            return 50.0
        
        centroid = numerator / denominator
        return round(centroid, 2)
    
    def get_category(self, stress_value):
        """Menentukan kategori stress berdasarkan nilai"""
        if stress_value < 20:
            return "Very Low Stress"
        elif stress_value < 40:
            return "Low Stress"
        elif stress_value < 60:
            return "Medium Stress"
        elif stress_value < 80:
            return "High Stress"
        else:
            return "Very High Stress"
    
    def get_message(self, stress_value, category):
        """Generate pesan berdasarkan kategori stress"""
        messages = {
            "Very Low Stress": "Kondisi sangat baik! Tetap jaga pola hidup sehat.",
            "Low Stress": "Kondisi baik. Pertahankan keseimbangan aktivitas digital.",
            "Medium Stress": "Perlu perhatian. Kurangi screen time dan perbaiki lingkungan.",
            "High Stress": "Kondisi tidak ideal. Segera istirahat dan perbaiki lingkungan sekitar.",
            "Very High Stress": "PERINGATAN! Segera kurangi penggunaan HP dan perbaiki kondisi ruangan!"
        }
        return messages.get(category, "Kondisi dalam pemantauan.")

# Instance global
fuzzy_system = FuzzyMamdani()

def calculate_stress(screen_hours, temperature, humidity, air_quality):
    """
    Fungsi utama untuk menghitung stress level
    
    Args:
        screen_hours: Durasi screen time dalam jam (0-24)
        temperature: Suhu ruangan dalam Celsius (15-35)
        humidity: Kelembaban dalam % (30-90)
        air_quality: Kualitas udara PM2.5 (0-5)
    
    Returns:
        dict: {
            'stress_value': float (0-100),
            'category': str,
            'message': str,
            'fuzzy_details': dict (untuk visualisasi)
        }
    """
    # Tahap 1: Fuzzifikasi
    fuzzy_values = fuzzy_system.fuzzification(
        screen_hours, temperature, humidity, air_quality
    )
    
    # Tahap 2: Inference (Aturan Fuzzy)
    activated_rules = fuzzy_system.inference(fuzzy_values)
    
    # Tahap 3: Defuzzifikasi
    stress_value = fuzzy_system.defuzzification(activated_rules)
    
    # Kategorisasi
    category = fuzzy_system.get_category(stress_value)
    message = fuzzy_system.get_message(stress_value, category)
    
    return {
        'stress_value': stress_value,
        'category': category,
        'message': message,
        'fuzzy_details': {
            'fuzzification': fuzzy_values,
            'activated_rules': activated_rules,
            'total_rules': len(activated_rules)
        }
    }

# Testing
if __name__ == "__main__":
    print("="*60)
    print("TESTING FUZZY MAMDANI SYSTEM (81 RULES)")
    print("="*60)
    
    # Test case 1: Kondisi ideal
    print("\n[TEST 1] Kondisi Ideal:")
    result = calculate_stress(2, 24, 60, 0.5)
    print(f"Input: Screen=2h, Temp=24°C, Humid=60%, AQ=0.5")
    print(f"Output: Stress={result['stress_value']} ({result['category']})")
    print(f"Rules aktif: {result['fuzzy_details']['total_rules']}")
    
    # Test case 2: Kondisi buruk
    print("\n[TEST 2] Kondisi Buruk:")
    result = calculate_stress(15, 32, 85, 4.5)
    print(f"Input: Screen=15h, Temp=32°C, Humid=85%, AQ=4.5")
    print(f"Output: Stress={result['stress_value']} ({result['category']})")
    print(f"Rules aktif: {result['fuzzy_details']['total_rules']}")
    
    # Test case 3: Kondisi medium
    print("\n[TEST 3] Kondisi Medium:")
    result = calculate_stress(7, 26, 65, 2.0)
    print(f"Input: Screen=7h, Temp=26°C, Humid=65%, AQ=2.0")
    print(f"Output: Stress={result['stress_value']} ({result['category']})")
    print(f"Rules aktif: {result['fuzzy_details']['total_rules']}")
    
    print("\n" + "="*60)
    print("Total aturan dalam sistem: 81 aturan")
    print("="*60)