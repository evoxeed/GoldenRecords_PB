from modules.validation import valid_cells
import time
# phoneFrame = pd.read_csv('dirty.csv', usecols=['contact_phone'])
# phoneFrame.to_csv('phone_output.txt', sep='\t', index=False)

dataasd = ['', 'vfbf', '3425', '5566']
start_time = time.time()
print(valid_cells(dataasd))
end_time = time.time()
print(f"Время выполнения программы: {end_time - start_time:.4f} секунд")